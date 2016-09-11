from minhash import DNAMinhashHandler
from postgres import BioPostgresWrapper
import abc


class SequenceLoader(object):
    """
    Abstract base class to load in sequencing
    information into a database
    """

    __metaclass__ = abc.ABCMeta

    sequence_chunk_size = 150

    @abc.abstractproperty
    def fasta_file_path(self):
        """
        Sequence loaders must define what
        fasta file they should be referencing
        """

    @abc.abstractproperty
    def sequence_table_name(self):
        """
        Sequence loaders must define what
        table to load the data into
        """

    @classmethod
    def load_sequence_into_db(cls, postgres_wrapper, element_sequence, minhash_array):
        postgres_wrapper.insert(cls.sequence_table_name, [
            ("sequence", element_sequence),
            ("hash", minhash_array)
        ])

    @classmethod
    def load_element_sequences(cls, element_sequences):
        """
        Takes an element sequence, constructs the minhash
        for it, and then loads it into the database
        """
        postgres_wrapper = BioPostgresWrapper()

        # iterates through the element sequences
        # loading them into the database
        for element_sequence in element_sequences:
            minhash_array = DNAMinhashHandler.generate_minhash_array(
                element_sequence)

            cls.load_sequence_into_db(postgres_wrapper, element_sequence, minhash_array)

        # commit the inserts to the database
        postgres_wrapper.commit_changes()

    @classmethod
    def load_sequence(cls):
        """
        Loads in sequences from a
        fasta file into the database
        """

        with open(cls.fasta_file_path) as fasta_file:
            fasta_contents = fasta_file.read()

            element_sequences = cls.get_element_sequences_from_content(
                fasta_contents)
            cls.load_element_sequences(element_sequences)


class GenomeSequenceLoader(SequenceLoader):

    fasta_file_path = "smallref.fa"
    sequence_table_name = "genome"

    @classmethod
    def get_element_sequences_from_content(cls, fasta_contents):
        """
        Turn the fasta contents into one giant sequence.
        Then, chunk the sequence into elements of size
        150
        """
        gene_sequence = "".join(fasta_contents.split()[1:])

        return [gene_sequence[chunk_index: chunk_index + cls.sequence_chunk_size]
                for chunk_index in xrange(0, len(gene_sequence), cls.sequence_chunk_size)]


class ReadSequenceLoader(SequenceLoader):

    fasta_file_path = "sample_fasta.fa"
    sequence_table_name = "sequence_input"

    @classmethod
    def get_element_sequences_from_content(cls, fasta_contents):
        """
        Determines which things in the contents are actually
        sequences and which are just weird positional
        information
        """
        return [element_string
                for element_string in fasta_contents.split()
                if ">" not in element_string]
