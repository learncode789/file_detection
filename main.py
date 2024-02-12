import base64
from google.cloud import storage
import os
import json
import datetime as dt
from datetime import datetime, timezone
from google.cloud import bigquery
from google.cloud import pubsub_v1
import pytz




project_name = os.environ.get("PROJECT_ID")
subject = str(os.environ.get("ENVIRONMENT")) + ": GCP File detection logs"
distribution_list = os.environ.get("EMAIL_TO")
topic_id = os.environ.get("ALERT_TOPIC")





def checking_24_hr_format_files(blobs, one_hour_ago):
    count = 0
    '''
    The code you provided retrieves all blobs (files) that were created in the last hour from a Google Cloud Storage (GCS) bucket 
    and prints their names
    '''
    print("******* Entered into checking_24_hr_format_files method **********")
    for blob in blobs:
        time_created = blob.time_created
        est_timezone = pytz.timezone('America/New_York')
        time_created = time_created.astimezone(est_timezone)
        print(blob.name,time_created,one_hour_ago)
        if blob.time_created >= one_hour_ago:
            count = count + 1
    return count


def checking_file_once(blobs, one_hour_ago):
    print("******* Entered into checking_file_once method **********")
    for blob in blobs:
        print("+++++++++++")
        print("Inside checking_file_once")
        time_created = blob.time_created
        est_timezone = pytz.timezone('America/New_York')
        time_created = time_created.astimezone(est_timezone)
        print((blob.name,time_created,one_hour_ago))
        if blob.time_created >= one_hour_ago:
            return True


def file_looking_process(event, context):
    global email_content
    print("********************* Initiating the process **************************************")
    client = bigquery.Client(project_name)
    query_job = client.query(
        "select file_name,bucket_name,directory_name,prefix,frequency_per_day,status,rec_time from sre.file_timings_table")
    result_dict = {key[0]: key[1:] for key in query_job}
    print("Results from the table \n{} ".format(result_dict))

    try:
        email_content = []
        client = storage.Client(project_name)
        expecetd_hour = None
        validTimeCheck = False

        print("********  Entering Into For Loop on  bigquery rows   ***************")
        row_number=0
        for filename, v in result_dict.items():
            row_number = row_number + 1
            print("RoW_Number : {0}  and their values ==> filename: {1} and Respective_Values : {2}".format(row_number,filename, v))
            bucket = client.get_bucket(v[0])

            if (v[2] != ""):
                blobs = bucket.list_blobs(prefix=str(filename))
            else:
                blobs = bucket.list_blobs(prefix=v[1] + '/' + str(filename))

            current_time = dt.datetime.now(dt.timezone.utc)
            est_timezone = pytz.timezone('America/New_York')
            est_time = current_time.astimezone(est_timezone)
            current_hour_minus_1 = est_time - dt.timedelta(hours=1)

            if ("," not in v[5]):
                print("----  Testing If condition  Cases ---- ")
                expecetd_hour = v[5].split("_")
                print("current_esttime-1 value {0}".format(current_hour_minus_1))
                print("If condition values : {0} {1} {2}".format(int(expecetd_hour[0]), int(current_hour_minus_1.hour),
                                                                 int(expecetd_hour[1])))
                print("If condition output = {0}".format(int(expecetd_hour[0]) <= int(current_hour_minus_1.hour)))
            else:
                pass

            print_str = ""
            if (v[3] > 23 and v[3] % 24 == 0):
                print("------  entered into  v[3] > 23 condition  --------")
                # Get all blobs that were created in the last hour
                count_1 = checking_24_hr_format_files(blobs, current_hour_minus_1)
                if (v[3] / 24 == count_1 and v[4] == 'Active'):
                    #print_str = print_str + f"""
                              #  BucketName: {v[0]}
                              #  File prefix: {v[1] + '/' + str(filename)}
                              #  Expected {str(v[3] / 24)} file(s) and found {str(count_1)} file(s)
                              #  SUCCESS MSG
                             #   """
                    print(print_str)
                else:
                    print_str = print_str + f"""
                                BucketName: {v[0]}
                                File prefix: {v[1] + '/' + str(filename)}
                                Expected {str(v[3] / 24)} file(s) and found {str(count_1)} file(s)
                                File Not found in Bucket
                                """
                    print(print_str)

            elif (expecetd_hour != None and int(v[3]) == 1 and v[4] == 'Active' and (int(expecetd_hour[0]) == 1) and (int(expecetd_hour[1]) == 24)):
                print("entered into file everyhour")
                output = checking_file_once(blobs, current_hour_minus_1)
                if (output == True):
                   # print_str = print_str + f"""
                              #  BucketName: {v[0]}
                              #  File prefix: {v[1] + '/' + str(filename)}
                              #  SUCCESS MSG
                             #  """
                    print(print_str)
                else:
                    print_str = print_str + f"""
                                 P1 BucketName: {v[0]}
                                 File prefix: {v[1] + '/' + str(filename)}
                                 File Not found in Bucket 
                                 """
                    print(print_str)        

            elif (expecetd_hour != None and int(v[3]) == 1 and v[4] == 'Active' and (int(expecetd_hour[0]) == int(current_hour_minus_1.hour))):
                print("------  entered into  v[3] ==1 condition  -------------")
                output = checking_file_once(blobs, current_hour_minus_1)
                if (output == True):
                   # print_str = print_str + f"""
                               # BucketName: {v[0]}
                                #File prefix: {v[1] + '/' + str(filename)}
                                #SUCCESS MSG
                                #"""
                    print(print_str)
                else:
                    print_str = print_str + f"""
                                 P1 BucketName: {v[0]}
                                 File prefix: {v[1] + '/' + str(filename)}
                                 File Not found in Bucket 
                                 """
                    print(print_str)

            elif (len(v[5].split(',')) > 1):
                # Get all blobs that were created by looking in rec_time(v[5])
                print("---------  entered into  random TimeZone  condition  ------------")
                res = 0
                msg_str = ""
                for rec_time in v[5].split(','):
                    print("****Start Loop*****")
                    rec_time = rec_time.split(":")
                    expecetd_hour = rec_time[0].split("_")
                    print(expecetd_hour)
                    print("Loop for the file {0} at this Timerange {1}".format(filename, expecetd_hour))
                    print("If condition values:{0}{1}{2}".format(int(expecetd_hour[0]),int(current_hour_minus_1.hour),int(expecetd_hour[1])))
                    print("If condition values={0}".format(int(expecetd_hour[0]) <= int(current_hour_minus_1.hour)))
                    if (expecetd_hour != None and v[4] == 'Active' and (int(expecetd_hour[0]) == int(current_hour_minus_1.hour))):
                        validTimeCheck = True
                        output = checking_24_hr_format_files(blobs, current_hour_minus_1)
                        msg_str=msg_str + \
                        "FileCount = {0} and expected count = {1}".format(output,rec_time[1]) +"  ,  "
                        print("FileCount = {0} and expected count = {1}".format(output,rec_time[1]))
                        if(output == int(rec_time[1])):
                            res = res + 1
                        else:
                            pass
                    print("****** End Loop*******\n")
                print("*************")
                print("file Match Result:",res)       

                if (validTimeCheck == True):
                    if(res > 0):
                        #print_str = print_str + f"""
                             #   BucketName: {v[0]}
                              #  File prefix: {v[1] + '/' + str(filename)}
                               # """
                        print(print_str)
                    else:
                        print_str = print_str + f"""
                                 P2 BucketName: {v[0]}
                                 File prefix: {v[1] + '/' + str(filename)}
                                 Log_info : {msg_str}
                                 File Not  found in Bucket 
                                 """
                        print(print_str)
            email_content.append(print_str)

    except Exception as e:
        exp_str = "Exception occured in start_archive_process: " + str(e)
        print(exp_str)
        email_content.append(exp_str)

    print("******************* Final Message ***************")
    print(email_content)
    send_email()
