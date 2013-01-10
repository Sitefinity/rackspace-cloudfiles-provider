using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Telerik.Sitefinity.BlobStorage;

namespace Sitefinity.CloudFiles.BlobStorage
{
    public static class CloudFilesExtensions
    {
        /// <summary>
        /// Sets the name in the CDN as {ID}.{Extension}
        /// </summary>
        /// <returns>Filename</returns>
        public static string GetFileName(this IBlobContentLocation content){
            return CloudFilesExtensions.CreateFileName(content.FileId, content.Extension);
        }

        /// <summary>
        /// Sets the name in the CDN as {ID}.{Extension}
        /// </summary>
        /// <returns>Filename</returns>
        public static string GetFileName(this IBlobContent content)
        {
            return CloudFilesExtensions.CreateFileName(content.FileId, content.Extension);
        }

        public static string CreateFileName(Guid fileId, string extension)
        {
            return String.Format("{0}{1}", fileId.ToString().ToLower(), extension.ToLower());
        }
    }
}
