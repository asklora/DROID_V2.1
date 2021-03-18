import sys
import paramiko as paramiko
from global_vars import EC2_hostname, EC2_username, EC2_model_key_file

def save_to_ec2(remote_file_path, model_path, model_filename):
    # This function saves the model to EC2
    try:
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print("connecting...")
        c.connect(hostname=EC2_hostname, username=EC2_username, key_filename=EC2_model_key_file)
        print("connected!")

        ftp_client = c.open_sftp()

        mkdir_p(ftp_client, remote_file_path)

        ftp_client.put(model_path + model_filename, remote_file_path + model_filename)
        ftp_client.close()
        c.close()
        print("Model saved to EC2!")
    except:
        sys.exit("Connection Failed!!!")


def load_from_ec2(remote_file_path, model_path, model_filename):
    # This function loads the model from EC2
    try:
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print("connecting...")
        c.connect(hostname=EC2_hostname, username=EC2_username, key_filename=EC2_model_key_file)
        print("connected!")

        ftp_client = c.open_sftp()
        ftp_client.get(remote_file_path + model_filename, model_path + model_filename)
        ftp_client.close()
        c.close()
        print("Model loaded from EC2!")
    except:
        sys.exit("Connection Failed!!!")
    # return model


def mkdir_p(sftp, remote_directory):
    # This function checks if the desired directory exists in Ec2. If not it will create it.
    dir_path = str()
    for dir_folder in remote_directory.split("/"):
        if dir_folder == "":
            continue
        dir_path += r"/{0}".format(dir_folder)
        try:
            sftp.listdir(dir_path)
        except IOError:
            sftp.mkdir(dir_path)
