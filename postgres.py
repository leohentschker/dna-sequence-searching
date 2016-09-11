from minhash import DNAMinhashHandler
import psycopg2
import abc


"""
Things that were done to initialize the database

GRANT ALL PRIVILEGES ON DATABASE bio TO bio;

CREATE TABLE genome(
    id serial primary key,
    sequence VARCHAR(200) not null,
    hash bigint[]);
GRANT ALL PRIVILEGES ON TABLE genome TO bio;

CREATE TABLE sequence_input(
    id serial primary key,
    sequence VARCHAR(200) not null,
    hash bigint[]);
GRANT ALL PRIVILEGES ON TABLE sequence_input TO bio;
"""


class PostgresWrapper(object):

    __metaclass__ = abc.ABCMeta

    # by default connect to the localhost
    host = "localhost"

    @abc.abstractproperty
    def dbname(self):
        """
        What is the name of the associated
        database?
        """

    @abc.abstractproperty
    def db_user(self):
        """
        What is the user on the database?
        """

    @abc.abstractproperty
    def db_password(self):
        """
        What is the password associated with
        the database user
        """

    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.db_user,
            host=self.host,
            password=self.db_password)

        self.cur = self.conn.cursor()

    def commit_changes(self):
        """
        Makes the changes persistent
        """
        self.conn.commit()
        self.conn.close()

    def insert(self, table_name, values_list):
        """
        Takes in a table to insert into and a
        dictionary of values mapping the column
        to what you want to put in that column
        """
        columns_string = ", ".join(column for column, value in values_list)
        values = tuple(value for column, value in values_list)
        values_placeholder = ",".join("%s" for _ in xrange(len(values)))

        insert_sql = "INSERT INTO {table_name} ({columns_string}) VALUES ({values_placeholder});" \
            .format(
                table_name=table_name,
                columns_string=columns_string,
                values_placeholder=values_placeholder)

        self.cur.execute(insert_sql, values)


class BioPostgresWrapper(PostgresWrapper):
    
    dbname = "bio"
    db_user = "bio"
    db_password = "bio"
    genome_table_name = "genome"
    sequence_input_table_name = "sequence_input"

    def search_for_sequence(self, base_sequence, table_name):
        """
        Takes in a base sequence and then 
        returns all of the items in the
        genome that match it
        """
        minhash_array = DNAMinhashHandler.generate_minhash_array(base_sequence)
        
        search_sql = "SELECT id, sequence FROM {table_name} WHERE hash && %(minhash)s;" \
            .format(table_name=table_name)

        self.cur.execute(search_sql, {"minhash": minhash_array})
        results = self.cur.fetchall()
        print results

    def search_genome(self, base_sequence):
        """
        Searches across the larger genome
        """
        return self.search_for_sequence(base_sequence, self.genome_table_name)

    def search_sequence_input(self, base_sequence):
        """
        Searches on the sequence_input table
        """
        return self.search_for_sequence(base_sequence, self.sequence_input_table_name)
