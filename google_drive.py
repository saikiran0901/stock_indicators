from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from apiclient import errors
from apiclient.http import MediaFileUpload

# If modifying these scopes, delete the file token.json.
#SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']
SCOPES = ['https://www.googleapis.com/auth/drive.appdata',
          'https://www.googleapis.com/auth/drive.file',
          'https://www.googleapis.com/auth/drive']


def googledriveupdate(service,file):

    #fileid='12s6BEXyX5OeFcq56mS2uZYirYZkNX3Tg'
    
#    folder_list = service.ListFile({'q': "trashed=false"}).GetList()
#    for folder in folder_list:
#         print('folder title: %s, id: %s' % (folder['title'], folder['id']))
         
    #file_metadata = {'name': file,'parents':['19rKclROFlEz_M0d8-Q2suijYjkeK3PDP']}
    file_metadata = {'name': file}
    file_id='1beyM35GmT4jSH8EaMiq7wU28wlz0_6PR'
    media = MediaFileUpload('{}'.format(file),
                            mimetype='application/vnd.ms-excel')
    file = service.files().update(body=file_metadata,
                                        media_body=media,
                                        fields='id',
                                        fileId=file_id).execute()
    
#    file = service.files().create(body=file_metadata,
#                                        media_body=media,
#                                        fields='id').execute()
    print('File ID: ' + file.get('id'))
    
    
def insert_file(service, title, description, parent_id, mime_type, filename):
  """Insert new file.

  Args:
    service: Drive API service instance.
    title: Title of the file to insert, including the extension.
    description: Description of the file to insert.
    parent_id: Parent folder's ID.
    mime_type: MIME type of the file to insert.
    filename: Filename of the file to insert.
  Returns:
    Inserted file metadata if successful, None otherwise.
  """
  print("Uploading the file")
  media_body = MediaFileUpload(filename, mimetype=mime_type, resumable=True)
  body = {
    'title': title,
    'description': description,
    'mimeType': mime_type
  }
  # Set the parent folder.
  if parent_id:
    body['parents'] = [{'id': parent_id}]

  try:
    file = service.files().update(
        body=body,
        convert=True,
        media_body=media_body).execute()

    # Uncomment the following line to print the File ID
    # print 'File ID: %s' % file['id']

    return file
  except errors.HttpError as error:
    print('An error occured:',error)
    return None

def main():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'drive_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('drive', 'v3', credentials=creds)

    # Call the Drive v3 API
    results = service.files().list(
        pageSize=10, fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])

    googledriveupdate(service,'indicators.xlsx')
#    file_metadata = {'name': 'indicators.xlsx'}
#    media = MediaFileUpload('indicators.xlsx', mimetype='application/vnd.google-apps.spreadsheet')
#    file = service.files().create(body=file_metadata,
#                                        media_body=media,
#                                        fields='id').execute()
#    print('File ID: %s' % file.get('id'))
    
    #insert_file(service,'X','X','Tradia','application/vnd.google-apps.spreadsheet','indicators.xlsx')
#    if not items:
#        print('No files found.')
#    else:
#    print('Files:')
#    for item in items:
#            print(u'{0} ({1})'.format(item['name'], item['id']))
            
            

if __name__ == '__main__':
    main()