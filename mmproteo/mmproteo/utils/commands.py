from typing import Dict, Set

import pandas as pd
from mmproteo.utils import log, pride, visualization, formats, utils
from mmproteo.utils.config import Config


class AbstractCommand:
    def get_command(self) -> str:
        raise NotImplementedError

    def get_description(self) -> str:
        raise NotImplementedError

    def run(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        raise NotImplementedError

    def validate(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        pass


class DownloadCommand(AbstractCommand):

    def get_command(self) -> str:
        return "download"

    def get_description(self) -> str:
        return "download files from a given project"

    def run(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        downloaded_files = pride.download(project_name=config.pride_project,
                                          valid_file_extensions=config.valid_file_extensions,
                                          max_num_files=config.max_num_files,
                                          download_dir=config.storage_dir,
                                          skip_existing=config.skip_existing,
                                          count_failed_files=config.count_failed_files,
                                          file_name_column=config.default_file_name_column,
                                          download_link_column=config.default_download_link_column,
                                          downloaded_files_column=config.default_downloaded_files_column,
                                          api_versions=config.pride_versions,
                                          logger=logger)
        if downloaded_files is None:
            return
        if config.processed_files is None:
            config.processed_files = downloaded_files
        else:
            config.processed_files = config.processed_files.append(downloaded_files, ignore_index=True)

        visualization.print_df(df=downloaded_files,
                               max_num_files=None,
                               shown_columns=config.shown_columns,
                               urlencode_columns=[config.default_download_link_column],
                               logger=logger)

    def validate(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        config.require_pride_project(logger=logger)

        if len(config.valid_file_extensions) != 0 and config.max_num_files % len(config.valid_file_extensions) != 0:
            logger.info(
                "max-num-files should be a multiple of the number of valid_file_extensions to make sure that "
                "files that belong together are also downloaded together")


class InfoCommand(AbstractCommand):

    def get_command(self) -> str:
        return "info"

    def get_description(self) -> str:
        return "request project information for a given project"

    def run(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        project_info = pride.info(project_name=config.pride_project, api_versions=config.pride_versions, logger=logger)
        if project_info is None:
            return
        print(project_info)

    def validate(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        config.require_pride_project(logger=logger)


class ListCommand(AbstractCommand):

    def get_command(self) -> str:
        return "list"

    def get_description(self) -> str:
        return "list files and their attributes in a given project"

    def run(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        project_files = pride.list_files(project_name=config.pride_project,
                                         api_versions=config.pride_versions,
                                         file_extensions=config.valid_file_extensions,
                                         logger=logger)
        # TODO cache project_files df for later downloading

        if project_files is None:
            return
        visualization.print_df(df=project_files,
                               max_num_files=config.max_num_files,
                               shown_columns=config.shown_columns,
                               urlencode_columns=[config.default_download_link_column],
                               logger=logger)

    def validate(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        config.require_pride_project(logger=logger)


class ExtractCommand(AbstractCommand):

    def get_command(self) -> str:
        return "extract"

    def get_description(self) -> str:
        return "extract all downloaded archive files or, if none were downloaded, those in the data directory. " \
               "Currently, the following archive formats are supported: " + \
               formats.get_string_of_extractable_file_extensions()

    def run(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        files = utils.merge_column_values(config.processed_files,
                                          [config.default_downloaded_files_column])

        if len(files) == 0:
            files = utils.list_files_in_directory(config.storage_dir)

        files = formats.filter_files_list(filenames=files,
                                          file_extensions=config.valid_file_extensions,
                                          max_num_files=config.max_num_files,
                                          sort=Config.default_filter_sort,
                                          drop_duplicates=Config.default_filter_drop_duplicates,
                                          logger=logger)
        extracted_files = formats.extract_files(filenames=files,
                                                skip_existing=config.skip_existing,
                                                logger=logger)
        result_df = pd.DataFrame(data=extracted_files, columns=[config.default_extracted_files_column])
        if config.processed_files is None:
            config.processed_files = result_df
        else:
            config.processed_files.append(result_df)


class ConvertRawCommand(AbstractCommand):

    def get_command(self) -> str:
        return "convertraw"

    def get_description(self) -> str:
        return "convert all downloaded or extracted raw files or, if none were downloaded or extracted, " \
               "those raw files in the data " \
               "directory, into the given thermo output format using the ThermoRawFileParser"

    def run(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        files = utils.merge_column_values(config.processed_files,
                                          [config.default_downloaded_files_column,
                                           config.default_extracted_files_column])

        if len(files) == 0:
            files = utils.list_files_in_directory(config.storage_dir)

        formats.start_thermo_docker_container(storage_dir=config.storage_dir,
                                              thermo_docker_container_name=Config.default_thermo_docker_container_name,
                                              thermo_docker_image=Config.default_thermo_docker_image,
                                              thermo_start_container_command_template=Config.
                                              default_thermo_start_container_command_template,
                                              logger=logger)

        converted_files = formats.convert_raw_files(filenames=files,
                                                    output_format=Config.default_thermo_output_format,
                                                    skip_existing=config.skip_existing,
                                                    thermo_docker_container_name=Config.
                                                    default_thermo_docker_container_name,
                                                    thermo_exec_command=Config.default_thermo_exec_command,
                                                    logger=logger)

        result_df = pd.DataFrame(data=converted_files, columns=[config.default_converted_mgf_files_column])
        if config.processed_files is None:
            config.processed_files = result_df
        else:
            config.processed_files.append(result_df)

        formats.stop_thermo_docker_container(
            thermo_docker_container_name=Config.default_thermo_docker_container_name,
            thermo_stop_container_command_template=Config.default_thermo_stop_container_command_template,
            logger=logger
        )


class Mgf2ParquetCommand(AbstractCommand):

    def get_command(self) -> str:
        return "mgf2parquet"

    def get_description(self) -> str:
        return "convert all downloaded, extracted, or converted mgf files into parquet format, or, " \
               "if no files were previously processed, convert the mgf files in the data directory"

    def run(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        files = utils.merge_column_values(config.processed_files,
                                          [config.default_downloaded_files_column,
                                           config.default_extracted_files_column,
                                           config.default_converted_mgf_files_column])

        if len(files) == 0:
            files = utils.list_files_in_directory(config.storage_dir)

        formats.convert_mgf_files_to_parquet(filenames=files,
                                             skip_existing=config.skip_existing,
                                             logger=logger)


class Mz2ParquetCommand(AbstractCommand):

    def get_command(self) -> str:
        return "mz2parquet"

    def get_description(self) -> str:
        return "merge and convert all downloaded or extracted mzid and mzml files into parquet format" \
               " or, if no files were previously processed, merge and convert the files in the data directory."

    def run(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        files = utils.merge_column_values(config.processed_files,
                                          [config.default_downloaded_files_column,
                                           config.default_extracted_files_column])

        if len(files) == 0:
            files = utils.list_files_in_directory(config.storage_dir)

        formats.merge_mzml_and_mzid_files_to_parquet(filenames=files,
                                                     skip_existing=config.skip_existing,
                                                     max_num_files=config.max_num_files,
                                                     logger=logger)


class CommandDispatcher:
    def __init__(self) -> None:
        self.__commands: Dict[str, AbstractCommand] = dict()

    def register(self, command: AbstractCommand) -> None:
        if command.get_command() in self.__commands.keys():
            raise ValueError("Command is already registered")
        self.__commands[command.get_command()] = command

    def get_command_names(self) -> Set[str]:
        return set(self.__commands.keys())

    @staticmethod
    def _pad_command(command: str, width: int) -> str:
        return command.ljust(width)

    def get_command_descriptions_str(self) -> str:
        longest_command_length = max([len(command) for command in self.get_command_names()])

        return '\n'.join(
            [self._pad_command(command.get_command(), longest_command_length) + " : " + command.get_description()
             for command in self.__commands.values()])

    def get_command(self, command_name: str) -> AbstractCommand:
        if command_name not in self.__commands.keys():
            raise NotImplementedError("%s is no known command")
        return self.__commands[command_name]

    def dispatch_commands(self, config: Config, logger: log.Logger = log.DUMMY_LOGGER) -> None:
        commands = [self.get_command(command_name) for command_name in config.commands]

        for command in commands:
            try:
                command.validate(config=config, logger=logger)
            except log.LoggedWarningException:
                pass
            except log.LoggedErrorException:
                return

        for command in commands:
            try:
                command.run(config=config, logger=logger)
            except log.LoggedWarningException:
                pass
            except log.LoggedErrorException:
                return


DISPATCHER = CommandDispatcher()
DISPATCHER.register(ConvertRawCommand())
DISPATCHER.register(DownloadCommand())
DISPATCHER.register(ExtractCommand())
DISPATCHER.register(InfoCommand())
DISPATCHER.register(ListCommand())
DISPATCHER.register(Mgf2ParquetCommand())
DISPATCHER.register(Mz2ParquetCommand())
