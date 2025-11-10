"""
Example of a custom DataFileStreamProcessor to make simple plots of intensity vs angle
for the example files used in the OpenMSIStream tutorial
"""

# imports
import datetime
from io import BytesIO
import matplotlib.pyplot as plt
from openmsistream import DataFileStreamProcessor

# prevent pyplot from trying to pop GUI windows
plt.switch_backend("Agg")


class XRDCSVPlotter(DataFileStreamProcessor):
    """
    Makes simple plots of XRD intensity vs. angle data based on the .csv files
    used in the OpenMSIStream tutorial
    """

    def _process_downloaded_data_file(self, datafile, lock):
        """
        Make plots for the data in the given file
        """
        try:
            # get the raw data from the file's bytestring
            data_lines = [
                line.decode().strip()
                for line in (BytesIO(datafile.bytestring)).readlines()
            ]
            # skip down to the "[Scan points]" block
            start_index = 0
            while data_lines[start_index] != "[Scan points]":
                start_index += 1
            # make sure the next line is the "Angle" and "Intensity" headings
            start_index += 1
            angle_heading, intensity_heading = data_lines[start_index].split(",")
            if angle_heading != "Angle" or intensity_heading != "Intensity":
                errmsg = (
                    'ERROR: expecting "Angle" and "Intensity" column headings in the '
                    f'"[Scan points]" block, but found {angle_heading} and '
                    f"{intensity_heading}!"
                )
                raise ValueError(errmsg)
            start_index += 1
            # read the rest of the lines in the file into lists of data points
            angles = []
            intensities = []
            for line in data_lines[start_index:]:
                angle, intensity = line.split(",")
                angles.append(float(angle))
                intensities.append(float(intensity))
            # plot the data points
            f, ax = plt.subplots()
            ax.scatter(x=angles, y=intensities)
            ax.set_xlabel("Angle")
            ax.set_ylabel("Intensity")
            ax.set_title(f"{datafile.filename} XRD data")
            # determine the output filepath and save the plot image
            plot_path = (
                self._output_dir
                / datafile.subdir_str
                / (datafile.filepath.stem + "_xrd_plot.png")
            )
            if not plot_path.parent.is_dir():
                plot_path.parent.mkdir(parents=True)
            f.savefig(plot_path, bbox_inches="tight")
            plt.close(f)
        except Exception as exc:
            return exc
        return None

    def _failed_processing_callback(self, datafile, lock):
        warnmsg = (
            f"WARNING: failed to make plots for {datafile.full_filepath}! "
            "The consumer will need to be rerun to re-read data from this file."
        )
        self.logger.warning(warnmsg)

    def _mismatched_hash_callback(self, datafile, lock):
        warnmsg = (
            f"WARNING: hash of content for {datafile.full_filepath} is not matched "
            "to what was originally uploaded! The consumer will need to be rerun "
            "to re-read data from this file."
        )
        self.logger.warning(warnmsg)

    @classmethod
    def run_from_command_line(cls, args=None):
        """
        Run the plot maker from the command line
        """
        # make the argument parser
        parser = cls.get_argument_parser()
        args = parser.parse_args(args=args)
        # make the plot maker
        init_args, init_kwargs = cls.get_init_args_kwargs(args)
        plot_maker = cls(*init_args, **init_kwargs)
        # start the plot maker running
        run_start = datetime.datetime.now()
        plot_maker.logger.info(
            f"Listening to the {args.topic_name} topic to find XRD .csv files and create plots"
        )
        (
            n_msgs_read,
            n_msgs_proc,
            n_files_proc,
            proc_filepaths,
        ) = plot_maker.process_files_as_read()
        plot_maker.close()
        run_stop = datetime.datetime.now()
        # shut down when that function returns
        plot_maker.logger.info(
            f"XRD CSV plot maker writing to {plot_maker._output_dir} shut down"
        )
        msg = f"{n_msgs_read} total messages were consumed"
        if n_files_proc > 0:
            msg += (
                f", {n_msgs_proc} messages were successfully processed,"
                f" and plots were made for {n_files_proc} files"
            )
        else:
            msg += f" and {n_msgs_proc} messages were successfully processed"
        msg += (
            f" from {run_start} to {run_stop}. Up to {cls.N_RECENT_FILES} most recently "
            "processed files:\n\t"
        )
        msg += "\n\t".join([str(fp) for fp in proc_filepaths])
        plot_maker.logger.info(msg)


def main(args=None):
    """
    Calls run from command line
    """
    XRDCSVPlotter.run_from_command_line(args)


if __name__ == "__main__":
    main()
