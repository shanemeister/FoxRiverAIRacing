import argparse
import base64
from datetime import date, datetime

def gen_race_identifier(course_cd, race_date, race_number):
    """ Generate a race identifier by encoding course code, race date, and race number. """
    
    try:
        if not course_cd or not race_date or race_number is None:
            raise ValueError("Invalid inputs: course_cd, race_date, and race_number must not be empty or None.")

        race_number_int = int(race_number)

        # Convert race_date to string if it's a date object
        if isinstance(race_date, date):
            race_date = race_date.strftime('%Y-%m-%d')

        # Remove dashes from race_date to get a clean string
        formatted_race_date = race_date.replace('-', '')

        # Concatenate data (course_cd + race_date + race_number)
        data = f"{course_cd.upper()}{formatted_race_date}{race_number_int:02d}"

        # Ensure the data is of correct format and length
        if len(data) < 10:
            raise ValueError(f"Generated data '{data}' seems too short, possible invalid inputs.")

        # Encode the data using Base64
        encoded_data = base64.b64encode(data.encode('utf-8')).decode('utf-8')

        return encoded_data

    except (ValueError, TypeError) as e:
        print(f"Error generating race identifier: {e}, course_cd: {course_cd}, race_date {race_date}, race_number: {race_number}")
        return None
    
import base64

def decode_race_identifier(encoded_data):
    try:
        # Decode the Base64 string back to the original string
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')

        # Extract race_date (next 8 digits), and race_number (last 2 digits)
        race_date = decoded_data[-10:-2]  # Last 10 characters are the date and race number
        race_number = decoded_data[-2:]  # Last 2 characters are the race number

        # Format race_date back to YYYY-MM-DD
        formatted_race_date = f"{race_date[:4]}-{race_date[4:6]}-{race_date[6:]}"

        # Now, the rest of the decoded data is the course_cd
        course_cd = decoded_data[:-10]

        return course_cd, formatted_race_date, int(race_number)

    except (ValueError, TypeError, base64.binascii.Error) as e:
        print(f"Error decoding race identifier: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Encode or decode race identifiers using base64.")
    subparsers = parser.add_subparsers(dest='command', help='Encode or Decode operations')

    # Encode sub-command
    encode_parser = subparsers.add_parser('encode', help='Encode course_cd, race_date, and race_number')
    encode_parser.add_argument('course_cd', type=str, help='Course code (e.g., BEL)')
    encode_parser.add_argument('race_date', type=str, help='Race date in format YYYY-MM-DD')
    encode_parser.add_argument('race_number', type=int, help='Race number')

    # Decode sub-command
    decode_parser = subparsers.add_parser('decode', help='Decode a base64 encoded race identifier')
    decode_parser.add_argument('encoded_data', type=str, help='Base64 encoded race identifier')

    args = parser.parse_args()

    if args.command == 'encode':
        encoded_result = gen_race_identifier(args.course_cd, args.race_date, args.race_number)
        if encoded_result:
            print(f"Encoded race identifier: {encoded_result}")

    elif args.command == 'decode':
        decoded_result = decode_race_identifier(args.encoded_data)
        if decoded_result:
            course_cd, race_date, race_number = decoded_result
            print(f"Decoded race identifier: Course Code: {course_cd}, Race Date: {race_date}, Race Number: {race_number}")

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
