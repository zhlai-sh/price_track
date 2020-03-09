cd ..

virtualenv env

source env/bin/activate

python3 setup.py install

python3 preprocess.py

deactivate