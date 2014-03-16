#!/usr/bin/python


from subprocess import Popen, PIPE


def test_ssh(master_ip, username, verbose):
    try:
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

            if verbose:
                print(err)
            raise ValueError
        return "SSH Successful"
    except (ValueError, OSError) as err:
        print("\nERROR: Could not communicate with master")
        print(err)
        raise
    except:
        print("Unknown error Occured")
        raise


if __name__ == "__main__":
    print(test_ssh("130.237.221.231", "ubuntu",  True))
