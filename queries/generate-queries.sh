cd /home/user/ntua_adis/DSGen-software-code-3.2.0rc1/query_templates

for i in `ls query*tpl`
do 
    echo $i;  
    echo "define _END = \"\";" >> $i
done
cd /home/user/ntua_adis/DSGen-software-code-3.2.0rc1/tools

touch query.temp # dsqgen puts all the queries from the templates dir whose name are specified in the -INPUT (qlist) file in one output file
for i in $(seq 1 1 99)
do
echo "query$i.tpl" > query.temp # to create each query in a different file, each time write a new template name into the qlist file
./dsqgen -DIRECTORY /home/user/ntua_adis/DSGen-software-code-3.2.0rc1/query_templates -INPUT query$i.temp -VERBOSE -SCALE 1 -DIALECT netezza -OUTPUT_DIR /home/user/ntua_adis/queries

done
rm query.temp