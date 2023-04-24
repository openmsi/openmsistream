if __name__=='__main__' :
    try :
        from openmsistream.data_file_io.actor.data_file_upload_directory import main
        main(['test_upload'])
    except Exception :
        import pathlib, traceback, datetime
        output_filepath = pathlib.Path(r"/Users/margareteminizer/Desktop/dmref_materials_project/openmsi/openmsistream/test/testing_service_ERROR_LOG.txt")
        with open(output_filepath,'a') as fp :
            timestamp = (datetime.datetime.now()).strftime("%Y-%m-%d at %H:%M:%S")
            fp.write(f'Error on {timestamp}. Exception:\n{traceback.format_exc()}')
        import sys
        sys.exit(1)
