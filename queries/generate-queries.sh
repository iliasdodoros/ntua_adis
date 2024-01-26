cd /home/user/ntua_adis/DSGen-software-code-3.2.0rc1/tools


touch query.temp # dsqgen puts all the queries from the templates dir whose name are specified in the -INPUT (qlist) file in one output file
for i in $(seq 1 1 99)
do
cat /home/user/ntua_adis/DSGen-software-code-3.2.0rc1/query_templates/query$i.tpl > query.temp # to create each query in a different file, each time write a new template name into the qlist file
./dsqgen -INPUT query.temp -VERBOSE -SCALE 10 -DIALECT netezza -OUTPUT_DIR /home/user/ntua_adis/queries

done
rm query.temp