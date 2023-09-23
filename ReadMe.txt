# Cara menjalankan
- build -t airflow-dibimbing:1.0.11 .
- docker compose -d

# File atau Folder dan penjelasannya
exercise_days_13 : berisi folder proyek dan file main.py digunakan airflow untuk menandakan file utama saat DAG dijalankan
|-data/0. raw/ : berisi file dataset csv yang nanti digunakan dalam proses exctract
|-data/1. extract/ : berisi file hasil extract dari dataset csv menjadi file json yang nanti digunakan untuk dalam proses transform
|-data/2. transform/ : berisi file hasil transformasi berbentuk json setelah proses extract data, 1 file transform mewakili 1 hasil analisa tersedia juga plot gambar hasil analisa
|-data/3. load/ : berisi file parquet hasil convert dari file json dari folder transform sebelumnya
|-process/extract/ : berisi file extract.py bertugas untuk convert dari folder file dataset csv menjadi file json untuk disimpan ke folder extract
|-process/transform/ : berisi file transform.py bertugas untuk transformasi analisa atau visualisasi data, hasil analisa disimpan dalam bentuk file json dan gambar 
|-process/load/ : berisi file load.py bertugas mengambil file json dari folder data transform dan diubah ke parquet dan disimpan ke folder data load

# Analisa saya
- BestOfCourse : hasil dari pencarian nama kursus berdasarkan jumlah murid, ini dapat digunakan untuk mencari mana kursus yang paling diminati
- BestOfAttendance : hasil dari pencarian kehadiran berdasarkan jumlah murid, ini dapat digunakan untuk mencari ditanggal berapa paling banyak murid yg hadir
- BestOfLecture : hasil dari pencairan perkuliahan berdasarkan jumlah murid, ini dapat digunakan untuk mencari dari mana saja perkuliahan yang paling banyak

note :
- file compose sudah berisi image dan service spark seperti spark master dan spark worker