from langdetect import detect, detect_langs
import re
import hashlib


def limit_string_tokens(input_string, max_words):
    words = input_string.split()  # Split the string into words based on spaces
    limited_words = words[:max_words]  # Take the first `max_words` words
    return " ".join(limited_words)  # Join them back into a string

def format_text_for_html(text: str):

    # Replace line breaks
    output_text = text.replace("\n", "<br/>")

    # Replace double stars with bold HTML tags
    output_text = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', output_text)
    return output_text


def create_hash_sha256(text: str = ''):
    # Create a hash object using SHA
    hash_object = hashlib.sha256()

    # Update the hash object with the text (must be encoded as bytes)
    hash_object.update(text.encode('utf-8'))

    # Get the hexadecimal digest of the hash
    hash_digest = hash_object.hexdigest()
    return hash_digest


def clean_markdown_json(response_content):
    """
    Removes Markdown code block tags (e.g., ```json) from the response content.
    """
    # Remove any code block markers from the beginning and end
    return re.sub(r"^```(?:json)?|```$", "", response_content.strip(), flags=re.MULTILINE)


def strtobool(val):
    """Convert a string representation of truth to true (1) or false (0).
    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))


def language_detection(text, method="single"):
    if (method.lower() != "single"):
      result = detect_langs(text)
    else:
      result = detect(text)

    return result


def clean_text(text):
    text = text.strip()
    if text == "":
        return text

    #  Cut off text after last dot
    index = text.rfind('.')
    output = ''
    if index > 0:
        output = text[0: index]

    #  Check if the last char is a dot
    if output[-1] != '.':
        output += '.'

    return output


def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def capitalize_first_word(sentence):
    # First, convert the entire sentence to lowercase
    lower_case_sentence = sentence.lower()

    # Next, use regular expressions to capitalize the first letter of each sentence
    capitalized_sentence = re.sub(r"(?<=\.\s)(\w+)", lambda x: x.group().capitalize(), lower_case_sentence.capitalize())

    return capitalized_sentence
