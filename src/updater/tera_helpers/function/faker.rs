use fake::faker::address::en::*;
use fake::faker::barcode::en::*;
use fake::faker::color::en::*;
use fake::faker::company::en::*;
use fake::faker::creditcard::en::*;
use fake::faker::currency::en::*;
use fake::faker::internet::en::*;
use fake::faker::job::en::*;
use fake::faker::lorem::en::*;
use fake::faker::name::en::*;
use fake::faker::number::en::*;
use fake::Fake;
use serde_json::value::Value;
use std::collections::HashMap;
use tera::*;

/// Generate words.
/// 
/// # Arguments
///
/// * `min` - Minumal number of word.
/// * `max` - Maximal number of word.
/// * `separator` - Separator to seperate 2 words.
/// 
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::words;
///
/// let args = HashMap::new();
/// let words = words(&args);
/// assert!(words.is_ok());
///
/// ```
pub fn words(args: &HashMap<String, Value>) -> Result<Value> {
    let min = args
        .get("min")
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(1);

    let max = args
        .get("max")
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(min + 1);

    if min >= max {
        return Err(Error::msg(
            "Function `words`: the argument `max` must be greater than `min`.",
        ));
    }

    let separator = args
        .get("separator")
        .and_then(Value::as_str)
        .unwrap_or(" ");

    let words = Words(min..max).fake::<Vec<String>>().join(separator);

    Ok(Value::String(words))
}

/// Generate sentences
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::sentences;
///
/// let args = HashMap::new();
/// let sentences = sentences(&args);
/// assert!(sentences.is_ok());
///
/// ```
pub fn sentences(args: &HashMap<String, Value>) -> Result<Value> {
    let min = args
        .get("min")
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(0);

    let max = args
        .get("max")
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(min + 1);

    if min >= max {
        return Err(Error::msg(
            "Function `sentences`: the argument `max` must be greater than `min`.",
        ));
    }

    let separator = args
        .get("separator")
        .and_then(Value::as_str)
        .unwrap_or(" ");

    let sentences = Sentences(min..max).fake::<Vec<String>>().join(separator);
    Ok(Value::String(sentences))
}

/// Generate paragraphs
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::paragraphs;
///
/// let args = HashMap::new();
/// let paragraphs = paragraphs(&args);
/// assert!(paragraphs.is_ok());
///
/// ```
pub fn paragraphs(args: &HashMap<String, Value>) -> Result<Value> {
    let min = args
        .get("min")
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(0);

    let max = args
        .get("max")
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(min + 1);

    if min >= max {
        return Err(Error::msg(
            "Function `paragraphs`: the argument `max` must be greater than `min`.",
        ));
    }

    let separator = args
        .get("separator")
        .and_then(Value::as_str)
        .unwrap_or("\n");

    let paragraphs = Paragraphs(min..max).fake::<Vec<String>>().join(separator);
    Ok(Value::String(paragraphs))
}

/// Generate first name
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::first_name;
///
/// let args = HashMap::new();
/// let first_name = first_name(&args);
/// assert!(first_name.is_ok());
///
/// ```
pub fn first_name(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(FirstName().fake()))
}

/// Generate last name
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::last_name;
///
/// let args = HashMap::new();
/// let last_name = last_name(&args);
/// assert!(last_name.is_ok());
///
/// ```
pub fn last_name(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(LastName().fake()))
}

/// Generate title
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::title;
///
/// let args = HashMap::new();
/// let title = title(&args);
/// assert!(title.is_ok());
///
/// ```
pub fn title(_args: &HashMap<String, Value>) -> Result<Value> {
    use fake::faker::name::en::*;
    Ok(Value::String(Title().fake()))
}

/// Generate job seniority
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::job_seniority;
///
/// let args = HashMap::new();
/// let job_seniority = job_seniority(&args);
/// assert!(job_seniority.is_ok());
///
/// ```
pub fn job_seniority(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Seniority().fake()))
}

/// Generate job field
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::job_field;
///
/// let args = HashMap::new();
/// let job_field = job_field(&args);
/// assert!(job_field.is_ok());
///
/// ```
pub fn job_field(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Field().fake()))
}

/// Generate job position
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::job_position;
///
/// let args = HashMap::new();
/// let job_position = job_position(&args);
/// assert!(job_position.is_ok());
///
/// ```
pub fn job_position(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Position().fake()))
}

/// Generate city
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::city;
///
/// let args = HashMap::new();
/// let city = city(&args);
/// assert!(city.is_ok());
///
/// ```
pub fn city(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(CityName().fake()))
}

/// Generate country name
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::country_name;
///
/// let args = HashMap::new();
/// let country_name = country_name(&args);
/// assert!(country_name.is_ok());
///
/// ```
pub fn country_name(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(CountryName().fake()))
}

/// Generate country code
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::country_code;
///
/// let args = HashMap::new();
/// let country_code = country_code(&args);
/// assert!(country_code.is_ok());
///
/// ```
pub fn country_code(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(CountryCode().fake()))
}

/// Generate street name
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::street_name;
///
/// let args = HashMap::new();
/// let street_name = street_name(&args);
/// assert!(street_name.is_ok());
///
/// ```
pub fn street_name(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(StreetName().fake()))
}

/// Generate state name
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::state_name;
///
/// let args = HashMap::new();
/// let state_name = state_name(&args);
/// assert!(state_name.is_ok());
///
/// ```
pub fn state_name(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(StateName().fake()))
}

/// Generate state code
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::state_code;
///
/// let args = HashMap::new();
/// let state_code = state_code(&args);
/// assert!(state_code.is_ok());
///
/// ```
pub fn state_code(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(StateAbbr().fake()))
}

/// Generate zipcode
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::zipcode;
///
/// let args = HashMap::new();
/// let zipcode = zipcode(&args);
/// assert!(zipcode.is_ok());
///
/// ```
pub fn zipcode(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(ZipCode().fake()))
}

/// Generate postcode
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::postcode;
///
/// let args = HashMap::new();
/// let postcode = postcode(&args);
/// assert!(postcode.is_ok());
///
/// ```
pub fn postcode(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(PostCode().fake()))
}

/// Generate timezone
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::timezone;
///
/// let args = HashMap::new();
/// let timezone = timezone(&args);
/// assert!(timezone.is_ok());
///
/// ```
pub fn timezone(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(TimeZone().fake()))
}

/// Generate latitude
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::latitude;
///
/// let args = HashMap::new();
/// let latitude = latitude(&args);
/// assert!(latitude.is_ok());
///
/// ```
pub fn latitude(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Latitude().fake()))
}

/// Generate longitude
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::longitude;
///
/// let args = HashMap::new();
/// let longitude = longitude(&args);
/// assert!(longitude.is_ok());
///
/// ```
pub fn longitude(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Longitude().fake()))
}

/// Generate profession
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::profession;
///
/// let args = HashMap::new();
/// let profession = profession(&args);
/// assert!(profession.is_ok());
///
/// ```
pub fn profession(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Profession().fake()))
}

/// Generate industry
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::industry;
///
/// let args = HashMap::new();
/// let industry = industry(&args);
/// assert!(industry.is_ok());
///
/// ```
pub fn industry(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Industry().fake()))
}

/// Generate email
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::email;
///
/// let args = HashMap::new();
/// let email = email(&args);
/// assert!(email.is_ok());
///
/// ```
pub fn email(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(FreeEmail().fake()))
}

/// Generate ipv4
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::ipv4;
///
/// let args = HashMap::new();
/// let ipv4 = ipv4(&args);
/// assert!(ipv4.is_ok());
///
/// ```
pub fn ipv4(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(IPv4().fake()))
}

/// Generate ipv6
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::ipv6;
///
/// let args = HashMap::new();
/// let ipv6 = ipv6(&args);
/// assert!(ipv6.is_ok());
///
/// ```
pub fn ipv6(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(IPv6().fake()))
}

/// Generate mac address
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::mac_address;
///
/// let args = HashMap::new();
/// let mac_address = mac_address(&args);
/// assert!(mac_address.is_ok());
///
/// ```
pub fn mac_address(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(MACAddress().fake()))
}

/// Generate hexadecimal color
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::color_hex;
///
/// let args = HashMap::new();
/// let color_hex = color_hex(&args);
/// assert!(color_hex.is_ok());
///
/// ```
pub fn color_hex(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Color().fake()))
}

/// Generate user agent
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::user_agent;
///
/// let args = HashMap::new();
/// let user_agent = user_agent(&args);
/// assert!(user_agent.is_ok());
///
/// ```
pub fn user_agent(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(UserAgent().fake()))
}

/// Generate digit
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::digit;
///
/// let args = HashMap::new();
/// let digit = digit(&args);
/// assert!(digit.is_ok());
///
/// ```
pub fn digit(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Digit().fake()))
}

/// Generate phone number
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::phone_number;
///
/// let mut args = HashMap::new();
/// let phone_number = phone_number(&args);
/// assert!(phone_number.is_ok());
///
/// ```
pub fn phone_number(args: &HashMap<String, Value>) -> Result<Value> {
    let default = "##########";

    let format = args
        .get("format")
        .and_then(Value::as_str)
        .unwrap_or(default);

    let phone: String = format.chars().map(|ch| {
        match ch {
            '^' => std::char::from_digit((1..10).fake(), 10).unwrap(),
            '#' => std::char::from_digit((0..10).fake(), 10).unwrap(),
            other => other,
        }
    }).collect();

    Ok(Value::String(phone))
}

/// Generate currency name
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::currency_name;
///
/// let mut args = HashMap::new();
/// let currency_name = currency_name(&args);
/// assert!(currency_name.is_ok());
///
/// ```
pub fn currency_name(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(CurrencyName().fake()))
}

/// Generate currency code
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::currency_code;
///
/// let mut args = HashMap::new();
/// let currency_code = currency_code(&args);
/// assert!(currency_code.is_ok());
///
/// ```
pub fn currency_code(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(CurrencyCode().fake()))
}

/// Generate currency symbol
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::currency_symbol;
///
/// let mut args = HashMap::new();
/// let currency_symbol = currency_symbol(&args);
/// assert!(currency_symbol.is_ok());
///
/// ```
pub fn currency_symbol(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(CurrencySymbol().fake()))
}

/// Generate credit card
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::credit_card;
///
/// let mut args = HashMap::new();
/// let credit_card = credit_card(&args);
/// assert!(credit_card.is_ok());
///
/// ```
pub fn credit_card(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(CreditCardNumber().fake()))
}

/// Generate barcode
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::barcode;
///
/// let mut args = HashMap::new();
/// let barcode = barcode(&args);
/// assert!(barcode.is_ok());
///
/// ```
pub fn barcode(_args: &HashMap<String, Value>) -> Result<Value> {
    Ok(Value::String(Isbn().fake()))
}

/// Generate password
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use chewdata::updater::tera_helpers::function::faker::password;
///
/// let args = HashMap::new();
/// let password = password(&args);
/// assert!(password.is_ok());
///
/// ```
pub fn password(args: &HashMap<String, Value>) -> Result<Value> {
    let min_value = match args.get("min") {
        Some(val) => try_get_value!("password", "min", Value, val),
        None => Value::default(),
    };

    let min = match min_value.as_u64() {
        Some(min) => min as usize,
        None => 0,
    };

    let max_value = match args.get("max") {
        Some(val) => try_get_value!("password", "max", Value, val),
        None => Value::default(),
    };

    let max = match max_value.as_u64() {
        Some(max) => max as usize,
        None => 10,
    };

    if min >= max {
        return Err(Error::msg(
            "Function `password` the argument `max` must be upper than the argument `min`",
        ));
    }

    Ok(Value::String(Password(min..max).fake()))
}
