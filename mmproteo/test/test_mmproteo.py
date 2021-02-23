#!/usr/bin/python3

import subprocess

DEFAULT_PROJECT = "PXD010000"


def _store_command_output(command: str, filename: str, include_stderr: bool = True) -> None:
    system_command = f"{command} > {filename}"
    if include_stderr:
        system_command += " 2>&1"
    subprocess.run(system_command, shell=True)


def _compare_stdout_with_file(command: str, filename: str, include_stderr: bool = True) -> None:
    if include_stderr:
        command += " 2>&1"

    process = subprocess.run(command, shell=True, stdout=subprocess.PIPE)
    stdout = process.stdout.decode('utf-8')
    stdout_lines = stdout.split("\n")

    stdout_lines = stdout_lines[:-1]  # remove empty trailing line

    with open(filename) as file:
        file_lines = file.readlines()
    file_lines = [line.rstrip("\n") for line in file_lines]

    assert len(stdout_lines) == len(file_lines)
    for received, expected in zip(stdout_lines, file_lines):
        assert received == expected


def store_mmproteo_output():
    _store_command_output(command="mmproteo",
                          filename="resources/mmproteo_output.txt")


def test_mmproteo_output():
    _compare_stdout_with_file(command="mmproteo",
                              filename="resources/mmproteo_output.txt")


def store_mmproteo_h_output():
    _store_command_output(command="mmproteo -h",
                          filename="resources/mmproteo_h_output.txt")


def test_mmproteo_h_output():
    _compare_stdout_with_file(command="mmproteo -h",
                              filename="resources/mmproteo_h_output.txt")


def store_mmproteo_p_info_output():
    _store_command_output(command=f"mmproteo -p {DEFAULT_PROJECT} info",
                          filename="resources/mmproteo_p_info_output.txt",
                          include_stderr=False)


def test_mmproteo_p_info_output():
    _compare_stdout_with_file(command=f"mmproteo -p {DEFAULT_PROJECT} info",
                              filename="resources/mmproteo_p_info_output.txt",
                              include_stderr=False)


def store_mmproteo_p_list_output():
    _store_command_output(command=f"mmproteo -p {DEFAULT_PROJECT} list",
                          filename="resources/mmproteo_p_list_output.txt",
                          include_stderr=False)


def test_mmproteo_p_list_output():
    _compare_stdout_with_file(command=f"mmproteo -p {DEFAULT_PROJECT} list",
                              filename="resources/mmproteo_p_list_output.txt",
                              include_stderr=False)


def store_mmproteo_p_n_list_output():
    _store_command_output(command=f"mmproteo -p {DEFAULT_PROJECT} -n 10 list",
                          filename="resources/mmproteo_p_n_list_output.txt",
                          include_stderr=False)


def test_mmproteo_p_n_list_output():
    _compare_stdout_with_file(command=f"mmproteo -p {DEFAULT_PROJECT} -n 10 list",
                              filename="resources/mmproteo_p_n_list_output.txt",
                              include_stderr=False)


if __name__ == '__main__':
    store_mmproteo_output()
    store_mmproteo_h_output()
    store_mmproteo_p_info_output()
    store_mmproteo_p_list_output()
    store_mmproteo_p_n_list_output()
