mkdir -p ./downloads/files/finals

if [ ! -f ./downloads/files/finals/asn.txt ]; then
    curl https://www.potaroo.net/bgp/iana/asn.txt > ./downloads/files/finals/asn.txt
fi

python3 download_files.py
cd downloads/files

echo 'executing ip2as...'
ip2as -p rib.prefixes -R rels-file -c cone-file -a as2org -P peeringdb.json -o ./finals/ip2as.prefixes

cd /app

echo 'updating DB...'
python3 ./update_db.py --asn_prefix_file_name ./downloads/files/finals/ip2as.prefixes --asn_to_name_file_name ./downloads/files/finals/asn.txt