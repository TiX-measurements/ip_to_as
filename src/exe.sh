mkdir -p ./downloads/files/finals
curl https://www.potaroo.net/bgp/iana/asn.txt > ./downloads/files/finals/asn.txt
python3 download_files.py
cd downloads/files
ip2as -p rib.prefixes -R rels-file -c cone-file -a as2org -P peeringdb.json -o ./finals/ip2as.prefixes

cd /app

python3 ./update_db.py --asn_prefix_file_name ./downloads/files/finals/ip2as.prefixes --asn_to_name_file_name ./downloads/files/finals/asn.txt