use csv::{ReaderBuilder, Trim, Writer};

use serde::{Deserialize, Serialize, Serializer};

use std::collections::HashMap;
use std::convert::TryFrom;
use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::ops::{Add, Sub};

/// Application errors.
#[derive(Debug)]
enum BankError {
    MissingAmount,
    Usage,
}

impl Display for BankError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let message = match &self {
            Self::MissingAmount => "Amount missing in transaction CSV",
            Self::Usage => "Command line usage error",
        };
        write!(f, "{}", message)
    }
}

impl Error for BankError {}

/// Transaction errors.
#[derive(Debug)]
enum TxError {
    InsufficientFunds,
    LockedAccount,
    NoSuchTransaction,
    Overflow,
}

impl Display for TxError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let message = match &self {
            Self::InsufficientFunds => "Insufficient funds",
            Self::LockedAccount => "Locked account",
            Self::NoSuchTransaction => "Referenced transaction not found",
            Self::Overflow => "Numerical overflow",
        };
        write!(f, "{}", message)
    }
}

impl Error for TxError {}

type ClientId = u16;
type TxId = u32;

/// Fixed-point money representation.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct Money(i64);

impl Add for Money {
    type Output = Result<Self, TxError>;

    fn add(self, rhs: Self) -> Self::Output {
        self.0
            .checked_add(rhs.0)
            .map(Self)
            .ok_or(TxError::Overflow)
    }
}

impl Sub for Money {
    type Output = Result<Self, TxError>;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0
            .checked_sub(rhs.0)
            .map(Self)
            .ok_or(TxError::Overflow)
    }
}

// TODO: Avoid f64, parse the decimal representation directly
impl From<f64> for Money {
    fn from(n: f64) -> Self {
        Self((n * 10_000.0).round() as i64)
    }
}

impl Display for Money {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let sign = if self.0 >= 0 { "" } else { "-" };
        let mag = self.0.unsigned_abs();
        let int_part = mag / 10_000;
        let frac_part = mag % 10_000;
        write!(f, "{}{}.{:04}", sign, int_part, frac_part)
    }
}

impl Serialize for Money {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// A transaction success result.
type TxResult<T = ()> = Result<T, TxError>;

/// Transaction details.
#[derive(Debug)]
enum Tx {
    Deposit {
        client: ClientId,
        id: TxId,
        amount: Money,
    },
    Withdrawal {
        client: ClientId,
        id: TxId,
        amount: Money,
    },
    Dispute {
        client: ClientId,
        id: TxId,
    },
    Resolve {
        client: ClientId,
        id: TxId,
    },
    Chargeback {
        client: ClientId,
        id: TxId,
    },
}

/// Transaction DTO.
#[derive(Debug, Deserialize)]
struct TxRecord {
    #[serde(rename = "type")]
    kind: String,
    client: ClientId,
    tx: TxId,
    amount: Option<f64>,
}

impl TryFrom<TxRecord> for Tx {
    type Error = BankError;

    fn try_from(record: TxRecord) -> Result<Self, Self::Error> {
        let TxRecord { kind, client, tx: id, amount } = record;

        let amount = amount.map(Money::from)
            .ok_or(BankError::MissingAmount);

        let tx = match kind.as_ref() {
            "deposit" => Tx::Deposit { client, id, amount: amount? },
            "withdrawal" => Tx::Withdrawal { client, id, amount: amount? },
            "dispute" => Tx::Dispute { client, id },
            "resolve" => Tx::Resolve { client, id },
            "chargeback" => Tx::Chargeback { client, id },
            _ => todo!(),
        };

        Ok(tx)
    }
}

#[derive(Debug)]
struct Account {
    client_id: ClientId,
    available: Money,
    held: Money,
    locked: bool,
}

impl Account {
    fn new(client_id: ClientId) -> Self {
        Self {
            client_id,
            available: Money(0),
            held: Money(0),
            locked: false,
        }
    }

    fn total_balance(&self) -> Money {
        (self.available + self.held)
            .expect("Overflow computing total balance")
    }

    fn check_unlocked(&self) -> TxResult {
        if self.locked {
            Err(TxError::LockedAccount)
        } else {
            Ok(())
        }
    }

    fn deposit(&mut self, amount: Money) -> TxResult {
        self.check_unlocked()?;
        self.available = (self.available + amount)?;

        Ok(())
    }

    fn withdraw(&mut self, amount: Money) -> TxResult {
        self.check_unlocked()?;

        let available = (self.available - amount)?;
        if available >= Money(0) {
            self.available = available;
            Ok(())
        } else {
            Err(TxError::InsufficientFunds)
        }
    }

    fn dispute(&mut self, amount: Money) -> TxResult {
        let available = (self.available - amount)?;
        let held = (self.held + amount)?;

        self.available = available;
        self.held = held;

        Ok(())
    }

    fn resolve(&mut self, amount: Money) -> TxResult {
        let available = (self.available + amount)?;
        let held = (self.held - amount)?;

        self.available = available;
        self.held = held;

        Ok(())
    }

    fn chargeback(&mut self, amount: Money) -> TxResult {
        self.held = (self.held - amount)?;
        self.locked = true;

        Ok(())
    }
}

/// Account DTO.
#[derive(Debug, Serialize)]
struct AccountRecord {
    client: ClientId,
    available: Money,
    held: Money,
    total: Money,
    locked: bool,
}

impl From<&Account> for AccountRecord {
    fn from(account: &Account) -> Self {
        Self {
            client: account.client_id,
            available: account.available,
            held: account.held,
            total: account.total_balance(),
            locked: account.locked,
        }
    }
}

/// Holds all the accounts and tracks transactions.
#[derive(Debug, Default)]
struct Bank {
    accounts: HashMap<ClientId, Account>,
    amounts: HashMap<TxId, Money>,
}

impl Bank {
    /// Look up an account by the client's ID number.
    fn account(&mut self, client: ClientId) -> &mut Account {
        self.accounts
            .entry(client)
            .or_insert_with(|| Account::new(client))
    }

    /// Get the amount associated with a previous transaction.
    fn amount(&self, id: TxId) -> TxResult<Money> {
        self.amounts
            .get(&id)
            .copied()
            .ok_or(TxError::NoSuchTransaction)
    }

    /// Process a single transaction.
    fn process(&mut self, tx: Tx) -> TxResult {
        match &tx {
            &Tx::Deposit { client, id, amount } => {
                self.account(client).deposit(amount)?;
                self.amounts.insert(id, amount);
                Ok(())
            }
            &Tx::Withdrawal { client, id: _, amount } => {
                self.account(client).withdraw(amount)
            }
            &Tx::Dispute { client, id } => {
                let amount = self.amount(id)?;
                self.account(client).dispute(amount)
            }
            &Tx::Resolve { client, id } => {
                let amount = self.amount(id)?;
                self.account(client).resolve(amount)
            }
            &Tx::Chargeback { client, id } => {
                let amount = self.amount(id)?;
                self.account(client).chargeback(amount)
            }
        }
    }

    /// Iterate over all the accounts.
    fn accounts(&self) -> impl Iterator<Item = &Account> {
        self.accounts.values()
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<OsString> = env::args_os().collect();
    if args.len() != 2 {
        Err(BankError::Usage)?;
    }

    let mut bank = Bank::default();

    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .trim(Trim::All)
        .from_path(&args[1])?;

    for (i, result) in reader.deserialize().enumerate() {
        let record: TxRecord = result?;
        let tx = Tx::try_from(record)?;

        match bank.process(tx) {
            Ok(_) => {},
            Err(e) => eprintln!("warning: transaction {} failed: {}", i + 1, e),
        }
    }

    let mut writer = Writer::from_writer(io::stdout());
    for account in bank.accounts() {
        writer.serialize(AccountRecord::from(account))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_money() {
        assert_eq!(Money(0).to_string(), "0.0000");
        assert_eq!(Money(1).to_string(), "0.0001");
        assert_eq!(Money(-1).to_string(), "-0.0001");
        assert_eq!(Money(12_345).to_string(), "1.2345");
        assert_eq!(Money(-12_345).to_string(), "-1.2345");
    }
}
