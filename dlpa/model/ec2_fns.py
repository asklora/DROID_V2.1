import sys

import paramiko as paramiko


def save_to_ec2(args):
    # This function saves the model to EC2
    try:
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print("connecting...")
        c.connect(hostname="13.209.141.35", username="seoul", key_filename="dlpa/model_saving_key.pem")
        # c.connect(hostname="15.165.8.208", username="seoul", key_filename="/home/loratech/PycharmProjects/DLPA/extra"
        #                                                                    "/model_saving_key.pem")
        print("connected!")

        ftp_client = c.open_sftp()

        mkdir_p(ftp_client, args.remote_file_path)
        # stdin, stdout, stderr = c.exec_command("mkdir %s"%(args.remote_file_path))

        ftp_client.put(args.model_path + args.model_filename, args.remote_file_path + args.model_filename)
        ftp_client.close()
        c.close()
        print("Model saved to EC2!")
    except:
        sys.exit("Connection Failed!!!")


def load_from_ec2(args):
    # This function loads the model from EC2
    try:
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print("connecting...")
        c.connect(hostname="13.209.141.35", username="seoul", key_filename="dlpa/model_saving_key.pem")
        # c.connect(hostname="15.165.8.208", username="seoul", key_filename="/home/loratech/PycharmProjects/DLPA/extra"
        #                                                                    "/model_saving_key.pem")
        print("connected!")

        ftp_client = c.open_sftp()
        ftp_client.get(args.remote_file_path + args.model_filename, args.model_path + args.model_filename)
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
