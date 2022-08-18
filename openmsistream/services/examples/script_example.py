#imports
import pathlib, datetime
from argparse import ArgumentParser

def main(args=None) :
    """
    The main function that will be run as a Service/daemon
    """
    parser = ArgumentParser()
    parser.add_argument('output_dir',type=pathlib.Path,
                        help='Path to the directory where the test file should be created')
    args = parser.parse_args(args)
    if not args.output_dir.is_dir() :
        if args.output_dir.exists() :
            raise ValueError(f'ERROR: given output directory {args.output_dir} exists but is not a directory!')
        args.output_dir.mkdir(parents=True)
    test_file_name = 'script_example_service_test.txt'
    if not (args.output_dir/test_file_name).is_file() :
        with open(args.output_dir/test_file_name,'w') as fp :
            fp.write('This file was created to test running a generic Python script as a Service/daemon')
    with open(args.output_dir/test_file_name,'a') as fp :
        fp.write(f'\nService rerun {(datetime.datetime.now()).strftime("on %Y-%m-%d at %H:%M:%S")}')

if __name__=='__main__' :
    main()