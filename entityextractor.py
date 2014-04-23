import MySQLdb;
import re
import nltk
from HTMLParser import HTMLParser
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()
batch = []
db = MySQLdb.connect(host="127.0.0.1.",user="root",passwd="123456",db="xataka1",port=3307)
done = True                                                                         ##done,offset:variables used for controlling loop
offset = 0
alltags = []                                                                        ##list used to store all the entities as well as post_id's
cur = db.cursor();
def entity_extractor(data):
    z=data.split()
    text = nltk.word_tokenize(data)
    
    tagged=nltk.pos_tag(text)                                       ##nltk.pos_tag() assigns tag to each token
    namedentities = nltk.chunk.ne_chunk(tagged,binary=True)
    entities=re.findall(r'NE\s(.*?)/',str(namedentities))
    entities=list(set(entities))
    entity1=[]
    for i in entities:
        for j in range(len(z)):
            if i==z[j]:
                try:
                
                    s=re.findall(r'\w',z[j+1])
                    s1=re.findall(r'\w',z[j+2])
                    s2=re.findall(r'\w',z[j+3])
                    m=len(s)
                    m1=len(s1)
                    m2=len(s2)
                    p=len(z[j+1])
                    p1=len(z[j+2])
                    p2=len(z[j+3])
##            if m!=p or m1!=p1 or m2!=p2:
##                continue
                    if m!=p:
                        continue
                    else:
                        entity1.append(i+' '+' '+z[j+1])
                    if m1!=p1:
                        continue
                    else:
                        entity1.append(i+' '+z[j+1]+' '+z[j+2])
                    if m2!=p2:
                        continue
                    else:
                        entity1.append(i+' '+z[j+1]+' '+z[j+2]+' '+z[j+3])
            
                except:
                    continue
    
    entity1=list(set(entity1))
    entities.extend(entity1)
    batch.extend("(%s, %s)" for i in range(len(entities)))
    
    return entities
while done:
    sql = "SELECT id, post_title, post_content FROM wp_posts where post_type = 'normal' and post_status = 'publish' LIMIT 100 OFFSET %d" %(offset)
    cur.execute(sql);
    data = cur.fetchall()
    #data=strip_tags(data[2])#this is for stripping the html tags
    #data = cur.fetchall()
    if data:                                                        ##data contains all post_id's,post_title's and post_content's
        for row in data:
            s=strip_tags(row[2])
            tags = entity_extractor(s)                         ##row[2] contains the post_content which is processed by entity_extractor() to extract entities from
            for tag in tags:                                        ##list of entities is returned from entity_extractor() and saved in tags
                alltags.append(tag)                                 ##each tag and post_id is stored in alltags list.
                alltags.append(row[0])
    else:
        done = False

    offset += 100;
    
cur.execute("CREATE TABLE tempf(entity_name varchar(200),post_id int)") 
cur.execute("CREATE TABLE post2entityf(post_id int,entity_id int)")
cur.execute("CREATE TABLE entitiesf(entity_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, entity_name VARCHAR(200))")
sql1 = "INSERT INTO tempf VALUES " + ",".join(batch)
#print alltags
cur.execute(sql1, alltags);                                           
#data=cur.fetchall()
#print data
#cur.execute("ALTER IGNORE TABLE tempf add unique index(entity_name,post_id)")

cur.execute("INSERT INTO entitiesf (entity_name) SELECT DISTINCT entity_name FROM tempf");

cur.execute("INSERT INTO post2entityf(post_id,entity_id) SELECT t.post_id,e.entity_id from tempf t,entitiesf e where t.entity_name = e.entity_name")

#cur.execute("DROP TABLE tempf")

db.commit()
cur.close()















