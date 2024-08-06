import os
from pathlib import Path


def save_to_file(df, filename):
    # Store the path in a cross-platform pathlib object to ensure compatibility
    # with DOS & UNIX-based operating systems.
    path = Path(filename)

    # Get the parent directory of the given path, if it exists.
    directory_path = str(path.parent.resolve())

    # If the given path points to a folder, attempt to create it. If it already
    # exists, the `exist_ok` option ensures that no exception will be thrown.
    if directory_path != "":
        os.makedirs(directory_path, exist_ok=True)

    # Export the data in a CSV file.
    try:
        df.to_csv(filename)
    except OSError as e:
        print('Unable to write on filesystem: {}'.format(e))
