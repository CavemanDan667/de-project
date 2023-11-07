def extract_event_data(event):
    file_name = event['Records'][0]['s3']['object']['key']
    now = file_name.split('/')[1][:-4]
    table = file_name.split('/')[0]

    return table, now