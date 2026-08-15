use core::str;
use rayon::prelude::*;
use std::io::BufReader;
use std::{error::Error, fs::File, print, println};

#[derive(Debug, Default)]
struct Person {
    name: Option<String>,
    surname: Option<String>,
    patronymic: Option<String>,
    birth_year: Option<String>,
    birth_month: Option<String>,
    birth_day: Option<String>,
    phone: Option<String>,
    mail: Option<String>,
    address: Option<String>,
    username: Option<String>,
    passport: Option<String>,
    snils: Option<String>,
    inn: Option<String>,
    other: Vec<(String, String)>,
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
    if digits.starts_with('7') || digits.starts_with('8') {
        format!("8{}", &digits[1..])
    } else {
        digits
    }
}

// fn fio(fio: Vec<String>) -> Vec<String>{

// }

fn birth(birth_day: &str) -> Vec<String> {
    birth_day.split('-').map(|s| s.to_string()).collect()
}

fn parse_row_to_person(row: &csv::ByteRecord) -> Person {
    let raw_birth = row
        .get(5)
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or("".to_string());
    let birth_days: Vec<String> = birth(&raw_birth);
    let (year, month, day) = match birth_days.as_slice() {
        [y, m, d] => (
            Some(y.to_string()),
            Some(m.to_string()),
            Some(d.to_string()),
        ),
        _ => (None, None, None),
    };
    let current_person = Person {
        phone: row
            .get(7)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .filter(|s| !s.is_empty())
            // .map(|s| phone(s))
            .filter(|p| !p.is_empty()),
        mail: row
            .get(8)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .filter(|s| !s.is_empty()),
        // .map(|s| s.to_string()),
        // fio
        surname: row
            .get(2)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .filter(|s| !s.is_empty()),
        // .map(|s| s.to_string()),
        name: row
            .get(3)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .filter(|s| !s.is_empty()),
        // .map(|s| s.to_string()),
        patronymic: row
            .get(4)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .filter(|s| !s.is_empty()),
        // birth
        birth_day: day,
        birth_month: month,
        birth_year: year,
        ..Default::default()
    };
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

fn main() {
    match db_reader("trash-dbs/test_full.csv") {
        Ok(()) => println!("Sucess"), //
        Err(err) => println!("error running example: {}", err),
    }
}
