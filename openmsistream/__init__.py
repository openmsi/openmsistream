import os
if os.name=='nt' :
    try :
        import confluent_kafka
    except Exception :
        import traceback
        try :
            import sys, pathlib
            dll_dir = pathlib.Path(sys.executable).parent/'Lib'/'site-packages'/'confluent_kafka.libs'
            if not dll_dir.is_dir() :
                raise FileNotFoundError(f'ERROR: expected DLL directory {dll_dir} does not exist!')
            from ctypes import CDLL
            fps = []
            for fp in dll_dir.glob('*.dll') :
                if fp.name.startswith('librdkafka') :
                    fps.append(fp)
                    CDLL(str(fp))
            if len(fps)<1 :
                raise RuntimeError(f'ERROR: {dll_dir} does not contain any librdkafka DLL files to preload!')
        except Exception :
            print(f'Failed to preload librdkafka DLLs. Exception: {traceback.format_exc()}')
        try :
            import confluent_kafka
        except Exception :
            errmsg = 'Preloading librdkafka DLLs ('
            for fp in fps :
                errmsg+=f'{fp}, '
            errmsg = f'{errmsg[:-2]}) did not allow confluent_kafka to be imported! Exception: '
            errmsg+= f'{traceback.format_exc()}'
            raise ImportError(errmsg)
    _ = confluent_kafka.Producer #appease pyflakes
