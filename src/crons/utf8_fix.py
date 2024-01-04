import os


def is_utf8(encoded_text):
    try:
        decoded_text = encoded_text.decode('utf-8')
        return True
    except UnicodeDecodeError:
        return False


def remove_non_utf8_lines(file_path):
    with open(file_path, 'rb') as file:
        lines = file.readlines()

    utf8_lines = [line for line in lines if is_utf8(line)]

    with open(file_path, 'wb') as file:
        file.writelines(utf8_lines)


def cleanup_files_in_directory(directory=None, tag=None):
    if os.path.exists(f'{os.path.join(directory, os.pardir, tag)}.cleaned'):
        return

    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            remove_non_utf8_lines(file_path)

    with open(f'{os.path.join(directory, tag)}.cleaned', 'w'):
        pass
