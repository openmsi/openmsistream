#imports
from ...running.runnable import Runnable
from ..metadata_json_reproducer import MetadataJSONReproducer

class XRDCSVMetadataReproducer(MetadataJSONReproducer,Runnable) :
    """
    An example class showing how to use a MetadataJSONReproducer to extract metadata from the header 
    of a .csv data file from an XRD measurement (read as chunks from a topic) and produce that metadata 
    as JSON to another topic
    """

    def _get_metadata_dict_for_file(self,datafile) :
        print('oh hi :3c')

def main(args=None) :
    XRDCSVMetadataReproducer.run_from_command_line(args)

if __name__=='__main__' :
    main()