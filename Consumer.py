
import csv
import pika
import time
import json

##################
# queue initialize
##################
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='testQu')
print(' [*] Waiting for messages. To exit press CTRL+C')


#############################
# processing output functions
#############################
def csv_output (outfilename, columns, results):
    # Create the csv file
    with open(outfilename, 'w', newline='') as f_handle:
        writer = csv.writer(f_handle)
        # Add the header/column names
        writer.writerow(columns)
        # Iterate over `results`  and  write to the csv file
        for row in results:
            writer.writerow(row)


def xml_output (outfilename, columns, results):
    outfile = open(outfilename, 'w')
    outfile.write('<?xml version="1.0" ?>\n')
    outfile.write('<mydata>\n')
    for row in results:
        outfile.write('  <row>\n')
        cells = row.split()
        for j, col in enumerate(columns):
            outfile.write('    <%s>%s</%s>\n' % (col, cells[j], col))
        outfile.write('  </row>\n')
    outfile.write('</mydata>\n')
    outfile.close()


def table_output (tablename, conn, cursor, query):
    cursor.execute('CREATE TABLE IF NOT EXISTS %s as %s' % (tablename, query))
    conn.commit()


def json_output (outfilename, columns ,results):
    items = [dict(zip(columns, row)) for row in results]
    json.dumps({'items': items})


#############################
# message processing function
#############################
def callback(ch, method, properties, body):
    import sqlite3
    print(" [x] Received %r" % body)
    db_path = body.split()[0].decode()
    output_tyoe = body.split()[1].decode()
    query = ['select t.name as track_name , composer, g.Name as genre_name from tracks t left join genres g on t.genreid=g.genreid;'
            ,'select firstName||'"' '"'||lastName as full_name, phone, email, address||'"', '"'||city||'"' ,'"'||state||'"' ,'"'||country||'"' ,'"'||postalcode as full_address ,total_albums from customers c left join (select customerid,count(distinct t.albumid) as total_albums from invoices i left join invoice_items ii on i.invoiceid = ii.invoiceid left join tracks t on ii.trackid = t.trackid group by 1  ) n on c.customerid = n.customerid;'
            ,'select country,domain,count(*) customers_in_domain from ( select  country, substr(email,instr(email,'"'@'"')+1,instr(substr( email, instr(email,'"'@'"'), length(email)),'"'.'"')-2) as domain from customers )country_domains group by 1,2;'
            ,'select country,sum(a) total_albums from( select c.country, t.albumid, COUNT(DISTINCT i.CUSTOMERID) as A  from invoices i left join customers c on i.customerid = c.customerid left join invoice_items ii on i.invoiceid=ii.invoiceid left join tracks t on ii.trackid = t.trackid  GROUP BY 1,2 ORDER BY 3 DESC ) group by 1;'
            ,'SELECT  s.country, case when a=max(a) then (select title from albums a where a.albumid=s.albumid ) end as best_seller_album from (select c.country, t.albumid, COUNT(DISTINCT i.CUSTOMERID) as A from invoices i left join invoice_items ii on i.invoiceid=ii.invoiceid left join tracks t on ii.trackid = t.trackid left join customers c on i.customerid = c.customerid GROUP BY 1,2 ORDER BY 3 DESC )s group by 1;'
            ,'select t.albumid albumid, count(distinct a.CUSTOMERID) total_copies from ( select i.invoiceid,i.customerid from invoices i where substr(i.invoicedate,1,4)>='"'2011'"' and i.customerid in (select customerid from customers where country='"'USA'"')) a left join invoice_items b on a.invoiceid = b.invoiceid left join tracks t on b.trackid=t.trackid group by 1 order by 2 desc limit 1;'
            ,'select  customerid, firstname, lastname, (case when firstname is null then 1 else 0 end) + (case when lastname is null then 1 else 0 end) + (case when company is null then 1 else 0 end) + (case when address is null then 1 else 0 end) + (case when city is null then 1 else 0 end) + (case when state is null then 1 else 0 end) + (case when country is null then 1 else 0 end) + (case when postalcode is null then 1 else 0 end) + (case when phone is null then 1 else 0 end)+ (case when fax is null then 1 else 0 end) + (case when email is null then 1 else 0 end) + (case when supportrepid is null then 1 else 0 end) as total_null from customers where total_null >=2 ;']
    db_connect = sqlite3.connect(db_path)
    cursor = db_connect.cursor()
    for i, x in enumerate(query):
        outfilename = 'query%d' % i
        if output_tyoe == "table":
            table_output(outfilename, db_connect, cursor, x)
            db_connect.commit()
        else:
            cursor.execute(x)
            columns = [i[0] for i in cursor.description]
            results = cursor.fetchall()
            if output_tyoe == "csv":
                csv_output(outfilename, columns, results)
            elif output_tyoe == "xml":
                xml_output(outfilename, columns, results)
            elif output_tyoe == "json":
                json_output(outfilename, columns, results)
            time.sleep(body.count(b'.'))
        print(" [x]  exec query ", x)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    cursor.close()
    db_connect.close()

#################
# queue handling
#################
channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
    queue='testQu')
channel.start_consuming()



