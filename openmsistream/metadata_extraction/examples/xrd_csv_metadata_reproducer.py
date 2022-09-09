"""Example of how to extend a MetadataJSONReproducer for a real-world use case"""

#imports
from datetime import datetime
from ..metadata_json_reproducer import MetadataJSONReproducer

class XRDCSVMetadataReproducer(MetadataJSONReproducer) :
    """
    An example class showing how to use a MetadataJSONReproducer to extract metadata from the header
    of a .csv data file from an XRD measurement (read as chunks from a topic) and produce that metadata
    as JSON to another topic
    """

    def _get_metadata_dict_for_file(self,datafile) :
        #get the string of the file from the bytestring
        data_as_str = datafile.bytestring.decode()
        #split it into lines
        lines = data_as_str.split('\n')
        #read until the "[Measurement conditions]" block
        iline=0
        line = lines[iline].strip()
        while line!='[Measurement conditions]' :
            iline+=1
            if iline>=len(lines) or line=='[Scan points]':
                errmsg = f'ERROR: could not find a [Measurement Conditions] block in the "{datafile.full_filepath}" '
                errmsg+=  'file from which to extract metadata!'
                raise RuntimeError(errmsg)
            line = lines[iline].strip()
        #read lines up to the "[Scan points]" block and add each key/value to the dictionary
        iline+=1
        line=lines[iline].strip()
        metadata_dict = {}
        while line!='[Scan points]' and iline<len(lines) :
            linesplit = line.split(',')
            if len(linesplit)==2 :
                k,v = linesplit
                if v=='' :
                    metadata_dict[k]=None
                else :
                    metadata_dict[k]=v
            elif len(linesplit)>2 :
                k = linesplit[0]
                vlist = []
                for v in linesplit[1:] :
                    if v=='' :
                        vlist.append(None)
                    else :
                        vlist.append(v)
                metadata_dict[k] = vlist
            iline+=1
            line=lines[iline].strip()
        if not metadata_dict :
            warnmsg = f'WARNING: [Measurement Conditions] block in the "{datafile.full_filepath}" file did not contain '
            warnmsg+=  'any metadata keys/values!'
            self.logger.warning(warnmsg)
        #add a timestamp
        metadata_dict['metadata_message_generated_at'] = datetime.now().strftime('%m/%d/%Y, %H:%M:%S')
        #return the dictionary of metadata
        return metadata_dict

def main(args=None) :
    """
    main function to run the extended class from the command line
    """
    XRDCSVMetadataReproducer.run_from_command_line(args)

if __name__=='__main__' :
    main()
