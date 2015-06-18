import os
import shutil
import hashlib
from flask import Flask
from flask.ext.sqlalchemy import SQLAlchemy
from dataserv.Validator import is_sha256


# Initialize the Flask application
app = Flask(__name__)
app.config.from_pyfile('config.py')
db = SQLAlchemy(app)


class DataFile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    file_hash = db.Column(db.String(128), unique=True)
    state = db.Column(db.Integer)
    byte_size = db.Column(db.Integer)

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
        """Press and move to data folder."""
        self.tmp_file = tmp_file
        self.file_hash = self.get_hash(True)

        # Generate path and filename for data folder
        data_path = os.path.join(app.config["DATA_DIR"], self.file_hash)

        # Get size of the file
        self.byte_size = os.path.getsize(data_path)

        # Move the file from processing to data
        shutil.move(self.tmp_file, data_path)

        db.session.add(self)
        db.session.commit()
