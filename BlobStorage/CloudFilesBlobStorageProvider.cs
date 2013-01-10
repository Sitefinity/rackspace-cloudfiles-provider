using System;
using System.Diagnostics;
using System.Linq;
using Telerik.Microsoft.Practices.EnterpriseLibrary.Logging;
using Telerik.Sitefinity.Modules.Libraries.BlobStorage;
using Telerik.Sitefinity.BlobStorage;
using System.Configuration;
using System.Collections.Specialized;
using System.IO;
using Rackspace.Cloudfiles;

namespace Sitefinity.CloudFiles.BlobStorage {
    public class CloudFilesBlobStorageProvider : CloudBlobStorageProvider { 
        /// <summary>
        /// Initializes the storage.
        /// </summary>
        /// <param name="config">The config.</param> 
        protected override void InitializeStorage(NameValueCollection config) {
            this._userName = config[Username].Trim();
            if (String.IsNullOrEmpty(this._userName))
                throw new ConfigurationException("'{0}' is required.".Arrange(Username));

            this._apiKey = config[ApiKey].Trim();
            if (String.IsNullOrEmpty(this._apiKey))
                throw new ConfigurationException("'{0}' is required.".Arrange(ApiKey));

            this._containerName = config[Container].Trim();
            if (String.IsNullOrEmpty(this._containerName))
                throw new ConfigurationException("'{0}' is required.".Arrange(Container));

            Debug.WriteLine("Authenticating...");
            this.Connection.Authenticate();
            Debug.WriteLine("Authenticated");

            var container = this.ContainerBucket;
            if (container != null)
            {
                Debug.WriteLine("Obtained the container");
                if(!this.ContainerBucket.CdnEnabled){
                    throw new CDNNotEnabledException(); //Cdn must be enabled to get the file
                }
            }else{
                throw new ContainerNotFoundException("Unable to find Container: {0}".Arrange(this._containerName)); //Can't find the container
            }
        }
        
        /// <summary>
        /// Uploads the specified content item to the remote blob storage.
        /// </summary>
        /// <param name="content">Descriptor of the item on the remote blob storage.</param>
        /// <param name="source">The source item's content stream.</param>
        /// <param name="bufferSize">Size of the upload buffer.</param>
        /// <returns>The length of the uploaded stream.</returns>
        public override long Upload(IBlobContent content, Stream source, int bufferSize)
        {
            var filename = this.GenerateFileName(content);
            var obj = new CF_Object(this.Connection, this.ContainerBucket, this.Client, filename);
            
            Debug.WriteLine("Saving File {0} to Cloudfiles", filename);
            obj.Write(source);
            Debug.WriteLine("File {0} saved to Cloudfiles with object name {1}", content.FilePath, filename);

            var uploadedObject = this.ContainerBucket.GetObject(filename);
            
            return uploadedObject.ContentLength;
        }



        /// <summary>
        /// Gets the upload stream.
        /// </summary>
        /// <param name="content">The content.</param>
        /// <returns></returns>
        public override Stream GetUploadStream(IBlobContent content) {
            throw new NotSupportedException("GetUploadStream() is not supported. Override Upload method.");
        }

        /// <summary>
        /// Gets the download stream.
        /// </summary>
        /// <param name="content">The content.</param>
        /// <returns></returns>
        public override Stream GetDownloadStream(IBlobContent content) {
            string filename = this.GenerateFileName(content);
            try
            {
                var cloudObject = this.ContainerBucket.GetObject(filename);
                return cloudObject.Read();
            }
            catch (ObjectNotFoundException oex)
            {
                Logger.Writer.Write("Object not found: Error obtaining the download stream for file {0} on the CDN as {1} CDN. {2}".Arrange(content.FilePath, filename, oex.Message));
            }catch(Exception ex){
                Logger.Writer.Write("Error obtaining the download stream for file {0} on the CDN as {1} CDN. {2}".Arrange(content.FilePath, filename, ex.Message));
            }

            return null;
        }

        /// <summary>
        /// Deletes the specified location.
        /// </summary>
        /// <param name="location">The location.</param>
        public override void Delete(IBlobContentLocation location) {
            string filename = this.GenerateFileName(location);
            try
            {
                this.ContainerBucket.DeleteObject(filename);
            }
            catch (ObjectNotFoundException oex)
            {
                Logger.Writer.Write("Object not found: Error deleting {0} from the cloudfiles CDN. {1}".Arrange(filename, oex.Message));
            }
        }

        /// <summary>
        /// BLOBs the exists.
        /// </summary>
        /// <param name="location">The location.</param>
        /// <returns></returns>
        public override bool BlobExists(IBlobContentLocation location) {
            string filename = this.GenerateFileName(location);
            try
            {
                var cloudObject = this.ContainerBucket.GetObject(filename);
                return true;
            }
            catch (ObjectNotFoundException oex)
            {
                return false;
            }
        }

        /// <summary>
        /// Gets the item URL.
        /// </summary>
        /// <param name="content">The content.</param>
        /// <returns></returns>
        public override string GetItemUrl(IBlobContentLocation content) {
            string filename = this.GenerateFileName(content);
            try{
                var cloudObject = this.ContainerBucket.GetObject(filename);

                return (cloudObject != null) ? cloudObject.CdnUri.AbsoluteUri : String.Empty;	
            }catch(ObjectNotFoundException oex){
                Logger.Writer.Write("Object not found: Error obtaining the Url for file {0} on the CDN as {1} CDN. {2}".Arrange(content.FilePath, filename, oex.Message));

                //######################################
                //TODO: FIX THIS!!!
                throw new ObjectNotFoundException("Object doesn't exist in cloudfiles for me to get a url...unsure how to handle this as the entire SF library service crashes");
                return "";
                //######################################
            }
        }

        /// <summary>
        /// Copies the specified source.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="destination">The destination.</param>
        public override void Copy(IBlobContentLocation source, IBlobContentLocation destination) {
            //Do nothing
        }


        /// <summary>
        /// Sets the properties.
        /// </summary>
        /// <param name="location">The location.</param>
        /// <param name="properties">The properties.</param>
        public override void SetProperties(IBlobContentLocation location, IBlobProperties properties) {
            //No properties to set by default
        }
        
        /// <summary>
        /// Gets the properties.
        /// </summary>
        /// <param name="location">The location.</param>
        /// <returns></returns>
        public override IBlobProperties GetProperties(IBlobContentLocation location) {
            //No properties to get by default
            return null;
        }

        public string GenerateFileName(IBlobContent content){
            return String.Format("{0}{1}", content.FileId.ToString().ToLower(), content.Extension.ToLower());
        }

        public string GenerateFileName(IBlobContentLocation content)
        {
            return String.Format("{0}{1}", content.FileId.ToString().ToLower(), content.Extension.ToLower());
        }

        #region Properties

        public const string Username = "username";
        public const string ApiKey = "apikey";
        public const string Container = "container";

        public CF_Client Client {
            get { 
                if (_client == null)
                    _client = new CF_Client();
                return _client; 
            }
            set { _client = value; }
        }

        public CF_Connection Connection {
            get {
                if (_connection == null)
                    _connection = new CF_Connection(this.UserCredentials);
                return _connection; 
            }
            set { _connection = value; }
        }


        public CF_Account Account {
            get { 
                if (_account == null)
                    _account = new CF_Account(this.Connection, this.Client);
                return _account; 
            }
            set { _account = value; }
        }

        public UserCredentials UserCredentials {
            get { 
                if (_userCredentials == null)
                    _userCredentials = new UserCredentials(this._userName, this._apiKey);
                return _userCredentials; 
            }
            set { _userCredentials = value; }
        }
        
        public Container ContainerBucket {
            get {
                if (_containerBucket == null)
                    _containerBucket = this.Account.GetContainer(this._containerName);
                return _containerBucket; 
            }
            set { _containerBucket = value; }
        }

        #endregion

        #region Fields

        private string _userName = "";
        private string _apiKey = "";
        private string _containerName = "";

        private UserCredentials _userCredentials = null;
        private CF_Client _client = null;
        private CF_Connection _connection = null;
        private CF_Account _account = null;
        private Container _containerBucket = null;
        #endregion
    }
}
