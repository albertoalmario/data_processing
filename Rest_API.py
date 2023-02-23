import os
from flask import Flask, request, render_template
from data_process import DataLoad
import parameters
from werkzeug.utils import secure_filename

app = Flask(__name__)

app.config["UPLOAD_FOLDER"] = parameters.SOURCE_FOLDER
ALLOWED_EXTENSIONS = set(['csv','txt'])
@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    file = request.files['file']
    print(file, file.filename)
    file_name = secure_filename(file.filename)
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], file_name))
    table_option = request.form['option']
    print('selected table ->', table_option)

    dataload = DataLoad(f'{parameters.SOURCE_FOLDER}/{file.filename}', table_option, 'csv')
    print(f'Loading table {table_option}....')
    dataload.csv_load()
    dataload.apply_rules()
    dataload.generate_erros_file(parameters.ERRORS_FOLDER)
    dataload.save_on_db()
    summary = (f'Source Count -> {dataload.data_frame.count()} \nErrors Count -> {dataload.errors_data_frame.count()} \nTarget Count -> {dataload.final_data_frame.count()}')
    print(summary)
    dataload.spark.stop()
    return f'File loaded:: \n {summary}'

@app.route('/')
def index():
    return render_template('index.html')

app.run(debug=True)