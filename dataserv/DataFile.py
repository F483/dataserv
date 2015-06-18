import os
import shutil
import hashlib
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from dataserv.Validator import is_sha256


# Initialize the Flask application
app = Flask(__name__)
app.config.from_pyfile('config.py')
db2 = SQLAlchemy(app)


class DataFile(db2.Model):
    id = db2.Column(db2.Integer, primary_key=True)
    file_hash = db2.Column(db2.String(128), unique=True)
    state = db2.Column(db2.Integer)
    byte_size = db2.Column(db2.Integer)

    def __init__(self):
        self.state = 0
        self.byte_size = 0
        self.tmp_file = ""

    def get_hash(self, ingest):
        """Get the SHA256 hash of the file."""
        if ingest:
            file_path = self.tmp_file
        else:
            file_path = app.config["DATA_DIR"] + self.file_hash

        hasher = hashlib.sha256()
        with open(file_path, 'rb') as afile:
            buf = afile.read()
            hasher.update(buf)
        return hasher.hexdigest()

    def is_sha256(self):
        return is_sha256(self.file_hash)

    def ingest_file(self, tmp_file):
        """Process and move to data folder."""
        self.tmp_file = tmp_file
        self.file_hash = self.get_hash(True)

        # Generate path and filename for data folder
        data_path = os.path.join(app.config["DATA_DIR"], self.file_hash)

        # Get size of the file
        self.byte_size = os.path.getsize(data_path)

        # Move the file from processing to data
        shutil.move(self.tmp_file, data_path)

        # Set state to 1
        self.state = 1

        db2.session.add(self)
        db2.session.commit()
