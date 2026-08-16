use clickhouse::{Client, Row};
use core::str;
use rayon::prelude::*;
use std::io::BufReader;
use std::path::Path;
use std::{error::Error, fs::File, print, println};
use std::{fs, io};

#[derive(Debug, Default, clickhouse::Row, serde::Serialize)]
// #[derive(Debug, Default)]
pub struct Person {
    pub phone: u64,
    pub email: String,
    pub username: String,

    pub surname: String,
    pub name: String,
    pub patronymic: String,
    pub birth_day: u8,
    pub birth_month: u8,
    pub birth_year: u16,

    pub passport: String,
    pub inn: String,
    pub snils: String,
    pub car_number: String,

    pub telegram_id: i64,
    pub source_id: String,

    pub extra: String, // Сюда закинем JSON со всем остальным
}

#[derive(Debug, clickhouse::Row, serde::Serialize)]
pub struct Meta {
    pub source_id: String,
    pub name: String,
    pub description: String,
    pub leak_date: String,
    pub record_count: u64,
    pub schema_mapping: String,
}

fn create_client() -> Client {
    Client::default()
        .with_url("http://127.0.0.0:8123")
        .with_user("default")
        .with_password("")
        .with_database("aura")
}

fn phone(phone_number: &str) -> String {
    // 1. Оставляем только цифры
    let digits: String = phone_number
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();

    // 2. Если строка пустая — отдаем как есть
    if digits.is_empty() {
        return digits;
    }

    // 3. Нормализуем первую цифру (7 или 8 переводим в 8)
    if digits.starts_with('7') || digits.starts_with('8') && digits.len() == 11 {
        return format!("7{}", &digits[1..]);
    } else {
        digits
    }
}

// fn fio(fio: Vec<String>) -> Vec<String>{

// }

fn parse_birth(birth_str: &str) -> (u16, u8, u8) {
    // Разбиваем по '-', '.', '/'
    let parts: Vec<&str> = birth_str
        .split(|c| c == '-' || c == '.' || c == '/')
        .collect();

    match parts.as_slice() {
        // Если дата пришла как YYYY-MM-DD
        [y, m, d] if y.len() == 4 => (
            y.parse::<u16>().unwrap_or(0),
            m.parse::<u8>().unwrap_or(0),
            d.parse::<u8>().unwrap_or(0),
        ),
        // Если дата пришла как DD-MM-YYYY
        [d, m, y] if y.len() == 4 => (
            y.parse::<u16>().unwrap_or(0),
            m.parse::<u8>().unwrap_or(0),
            d.parse::<u8>().unwrap_or(0),
        ),
        _ => (0, 0, 0),
    }
}

fn parse_row_to_person(row: &csv::ByteRecord) -> Person {
    let raw_birth = row
        .get(5)
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    let (year, month, day) = parse_birth(&raw_birth);

    let current_person = Person {
        phone: row
            .get(7)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.trim())
            .map(|s| phone(s))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0),
        email: row
            .get(8)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.trim().to_lowercase())
            .unwrap_or_default(),
        surname: row
            .get(2)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.trim().to_lowercase())
            .unwrap_or_default(),
        // .map(|s| s.to_string()),
        name: row
            .get(3)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.trim().to_lowercase())
            .unwrap_or_default(),
        // .map(|s| s.to_string()),
        patronymic: row
            .get(4)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(|s| s.trim().to_lowercase())
            .unwrap_or_default(),
        // birth
        birth_day: day,
        birth_month: month,
        birth_year: year,

        ..Default::default()
    };

    let used = [2, 3, 4, 7, 8];
    current_person
}

fn db_reader(db_name: &str) -> Result<(), Box<dyn Error>> {
    println!("Начало читки файла!");
    let mut raw_chunk: Vec<csv::ByteRecord> = Vec::with_capacity(100_000);
    let db = File::open(db_name)?;
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(b';')
        .from_reader(BufReader::with_capacity(4 * 1024 * 1024, db));

    let mut record = csv::ByteRecord::new();

    while rdr.read_byte_record(&mut record)? {
        raw_chunk.push(record.clone());
        if raw_chunk.len() == 100_000 {
            println!("Пошла обработка миллиона!");
            let processed_chunk: Vec<Person> = raw_chunk
                .par_iter()
                .map(|row| parse_row_to_person(row))
                .collect();

            println!(
                "Чанк на {} строк успешно обработан в многопотоке!",
                processed_chunk.len()
            );

            let client = create_client();
            push_to_click_db(client, processed_chunk);

            println!("Чанк загружен в DB!");

            raw_chunk.clear();
        }
    }
    if !raw_chunk.is_empty() {
        println!("Пошла обработка 10000!");
        let processed_chunk: Vec<Person> = raw_chunk
            .par_iter()
            .map(|record| parse_row_to_person(record))
            .collect();
        println!(
            "Чанк на {} строк успешно обработан в многопотоке!",
            processed_chunk.len()
        );
        raw_chunk.clear();
    }

    Ok(())
}

async fn push_to_click_db(client: &Client, push_vec: Vec<Person>) {
    let mut insert = client.insert("push_vec");
    for record in push_vec {
        insert.write(record).await?;
    }
    insert.end().await?;
}

async fn get_meta(name: String, description: String, leak_date: String) {}

fn ask(prompt: &str) -> String {
    print!("{}", prompt);
    let mut s = String::new();
    std::io::stdin().read_line(&mut s).ok();
    s.trim().to_string()
}

async fn main() {
    let name = ask("name:");
    let description = ask("description:");
    let leak_date = ask("leak_date (YYYY-MM-DD):");
    let db_path = ask("DB path (trash-dbs/cdek2021/):");
    let source_id = Path::new(&db_path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");

    println!("Start parsing!");
    let mut is_first: bool = true;
    for part in fs::read_dir(db_path)? {
        if is_first {
            let mut rdr = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_path(&part)?;
            let headers = rdr.headers().to_str();
        }
        is_first = false;
        db_reader(&(part.path().is_file()));
    }

    // match db_reader("trash-dbs/test_full.csv") {
    //     Ok(()) => println!("Sucess"),
    //     Err(err) => println!("error running example: {}", err),
    // }
}
