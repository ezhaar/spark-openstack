#!/usr/bin/python


from subprocess import Popen, PIPE
import sys

def scp(vm_ip, remote_username, src, dest, verbose=False):

    try:
        dest = remote_username + "@" + vm_ip + ":" + dest
        print ("copying to " + dest)
        out = Popen(["scp", src, dest],
                    stdout=PIPE,
                    stderr=PIPE)
        return out
    except (OSError, ValueError) as err:
        print("Could not login...\n Use verbose for detailed error msg")
        if verbose:
            print(err)
        sys.exit(0)
    except:
        print("Unknown error Occured")
        raise

if __name__ == "__main__":
    scp("130.237.221.238", "ubuntu", "/tmp/slaves", "/home/ubuntu")
