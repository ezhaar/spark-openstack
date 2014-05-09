#!/usr/bin/python


from subprocess import Popen, PIPE


def test_ssh(master_ip, username, verbose):
    result = Popen(["./helpers/utilities/hostkey.sh", master_ip],
                   stdout=PIPE,
                   stderr=PIPE)
    out, err = result.communicate()

    result = Popen(["./helpers/utilities/test_ssh.sh",
                    username, master_ip],
                    stdout=PIPE,
                    stderr=PIPE)  # .communicate()[0].strip("\n")
    out, err = result.communicate()

    if err:
         raise Exception
    else:
        return "SSH Successful"


