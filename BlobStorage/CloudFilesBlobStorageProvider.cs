using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using OpenStack.Swift;
using Telerik.Microsoft.Practices.EnterpriseLibrary.Logging;
using Telerik.Sitefinity.Modules.Libraries.BlobStorage;
using Telerik.Sitefinity.BlobStorage;
using System.Configuration;
using System.Collections.Specialized;
using System.IO;
using Rackspace.Cloudfiles;
using Rackspace.Cloudfiles.Constants;
using System.Net;

namespace Sitefinity.CloudFiles.BlobStorage {
    public class CloudFilesBlobStorageProvider : CloudBlobStorageProvider {
        public const string USERNAME = "username";
        public const string API_KEY = "apikey";
        public const string CONTAINER = "container";

        private string _userName = "";
        private string _apiKey = "";
        private string _containerName = "";

        private UserCredentials _userCredentials = null;
        private CF_Client _client = null;
        private CF_Connection _connection = null;
        private CF_Account _account = null;
        private Container _containerBucket = null;


        /// <summary>
        /// Initializes the storage.
        /// </summary>
        /// <param name="config">The config.</param> 
        protected override void InitializeStorage(NameValueCollection config) {
            this._userName = config[USERNAME].Trim();
            if (String.IsNullOrEmpty(this._userName))
                throw new ConfigurationException("'{0}' is required.".Arrange(USERNAME));

            this._apiKey = config[API_KEY].Trim();
            if (String.IsNullOrEmpty(this._apiKey))
                throw new ConfigurationException("'{0}' is required.".Arrange(API_KEY));

            this._containerName = config[CONTAINER].Trim();
            if (String.IsNullOrEmpty(this._containerName))
                throw new ConfigurationException("'{0}' is required.".Arrange(CONTAINER));

            //Connect to CloudFiles
            this.Connection.Authenticate();

            //This will all crap out unless this container is enabled
            var container = this.ContainerBucket;
            if (container != null)
            {
                if(!this.ContainerBucket.CdnEnabled){
                    throw new CDNNotEnabledException("Please log into your cloud account and enable CDN support on this container."); //Cdn must be enabled to get the file
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
        public override long Upload(IBlobContent content, Stream source, int bufferSize) {
            var filename = this.GetBlobName(content);
            var obj = new CF_Object(this.Connection, this.ContainerBucket, this.Client, filename);

            obj.Write(source);

            return obj.ContentLength;
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
            string filename = this.GetBlobName(content);
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
            try
            {
                this.ContainerBucket.DeleteObject(this.GetBlobPath(location));
            }
            catch (ObjectNotFoundException oex)
            {
                Logger.Writer.Write("Object not found: Error deleting {0} from the cloudfiles CDN. {1}".Arrange(location.FilePath, oex.Message));
            }
        }

        /// <summary>
        /// BLOBs the exists.
        /// </summary>
        /// <param name="location">The location.</param>
        /// <returns></returns>
        public override bool BlobExists(IBlobContentLocation location) {
            try
            {
                var blob = this.GetBlob(location);
                return (blob == null) ? false : true; //Not sure why I'm bothering, it'll throw an exception if there's no object
            }
            catch (ObjectNotFoundException oex)
            {
                Logger.Writer.Write("Object not found: Checking if Blob {0} Exists on the cloudfiles CDN. {1}".Arrange(location.FilePath, oex.Message));
                return false;
            }
        }

        /// <summary>
        /// Gets the item URL.
        /// </summary>
        /// <param name="content">The content.</param>
        /// <returns></returns>
        public override string GetItemUrl(IBlobContentLocation content) {
            try{
                var cloudObject = this.GetBlob(content);

                return (cloudObject != null) ? cloudObject.CdnUri.AbsoluteUri : String.Empty;	
            }catch(ObjectNotFoundException oex){
                Logger.Writer.Write("Object not found: Error obtaining the Url for file {0} on the CDN as {1} CDN. {2}".Arrange(content.FilePath, content.FilePath, oex.Message));
                return "http://127.0.0.1/{0}".Arrange(content.FilePath); //Cant return nothing, service will die
            }
        }

        /// <summary>
        /// Copies the specified source.
        /// </summary>
        /// <param name="source">Source is the new file uploaded to the cloud, Delete will be called immediatly after copy completes</param>
        /// <param name="destination">The existing object</param>
        public override void Copy(IBlobContentLocation source, IBlobContentLocation destination) {
            try
            {
                var sourceBlobPath = this.GetBlobPath(source);
                var destBlobPath = this.GetBlobPath(destination);

                this.ContainerBucket.CopyObject(sourceBlobPath, destBlobPath);
            }
            catch(Exception ex){
                Logger.Writer.Write("Error Copying Source File: {0} to Destination {1} on the CDN".Arrange(source.FilePath, destination.FilePath, ex.Message));
            }
        }

        public override void Move(IBlobContentLocation source, IBlobContentLocation destination)
        {
            this.Copy(source, destination);
            this.Delete(source);
        }


        /// <summary>
        /// Sets the properties.
        /// </summary>
        /// <param name="location">The location.</param>
        /// <param name="properties">The properties.</param>
        public override void SetProperties(IBlobContentLocation location, IBlobProperties properties) {
            //??
        }
        
        /// <summary>
        /// Gets the properties.
        /// </summary>
        /// <param name="location">The location.</param>
        /// <returns></returns>
        public override IBlobProperties GetProperties(IBlobContentLocation location) {
            var blob = this.GetBlob(location);

            return new BlobProperties
            {
                ContentType = blob.ContentType
            };
        }

        private StorageObject GetBlob(IBlobContentLocation blobLocation)
        {
            return this.ContainerBucket.GetObject(this.GetBlobPath(blobLocation));
        }

        private string GetBlobPath(IBlobContentLocation content)
        {
            return this.GetBlobName(content);
        }

        #region Properties
        /// <summary>
        /// Instance of the client
        /// </summary>
        protected CF_Client Client {
            get { 
                if (_client == null)
                    _client = new CF_Client();
                return _client; 
            }
            set { _client = value; }
        }

        /// <summary>
        /// CloudFiles Connection Object
        /// </summary>
        protected CF_Connection Connection {
            get {
                if (_connection == null)
                    _connection = new CF_Connection(this.UserCredentials);
                return _connection; 
            }
            set { _connection = value; }
        }

        /// <summary>
        /// Account for the user allowed to access the CDN
        /// </summary>
        protected CF_Account Account {
            get { 
                if (_account == null)
                    _account = new CF_Account(this.Connection, this.Client);
                return _account; 
            }
            set { _account = value; }
        }

        /// <summary>
        /// User allowed to access the CDN
        /// </summary>
        protected UserCredentials UserCredentials {
            get { 
                if (_userCredentials == null)
                    _userCredentials = new UserCredentials(this._userName, this._apiKey);
                return _userCredentials; 
            }
            set { _userCredentials = value; }
        }
        
        /// <summary>
        /// Instance of the CloudFiles Container
        /// </summary>
        protected Container ContainerBucket {
            get {
                if (_containerBucket == null)
                    _containerBucket = this.Account.GetContainer(this._containerName);
                return _containerBucket; 
            }
            set { _containerBucket = value; }
        }

        #endregion

    }
}
