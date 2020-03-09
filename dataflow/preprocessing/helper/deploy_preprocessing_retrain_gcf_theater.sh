cd ..

virtualenv env

source env/bin/activate

python3 setup.py install

python3 gcf_preprocess_theater.py

deactivate