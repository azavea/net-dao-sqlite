// Copyright (c) 2004-2010 Azavea, Inc.
// 
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
// 
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

using System;
using System.Data.SQLite;
using System.IO;
using Azavea.Open.DAO.SQL;

namespace Azavea.Open.DAO.SQLite
{
    /// <summary>
    /// Implements a FastDao layer customized for PostGreSQL (optionally with PostGIS installed).
    /// </summary>
    public class SQLiteDaLayer : SqlDaDdlLayer
    {
        /// <summary>
        /// Construct the layer.  Should typically be called only by the appropriate
        /// ConnectionDescriptor.
        /// </summary>
        /// <param name="connDesc">Connection to the Firebird DB we'll be using.</param>
        public SQLiteDaLayer(SQLiteDescriptor connDesc)
            : base(connDesc, true) { }

        #region Implementation of IDaDdlLayer

        /// <summary>
        /// Returns the DDL for the type of an automatically incrementing column.
        /// Some databases only store autonums in one col type so baseType may be
        /// ignored.
        /// </summary>
        /// <param name="baseType">The data type of the column (nominally).</param>
        /// <returns>The autonumber definition string.</returns>
        protected override string GetAutoType(Type baseType)
        {
            return "INTEGER PRIMARY KEY AUTOINCREMENT";
        }

        /// <summary>
        /// Returns the SQL type used to store a byte array in the DB.
        /// </summary>
        protected override string GetByteArrayType()
        {
            return "BLOB";
        }

        /// <summary>
        /// Returns the SQL type used to store a long in the DB.
        /// </summary>
        protected override string GetLongType()
        {
            return "INTEGER";
        }

        /// <summary>
        /// Returns true if you need to call "CreateStoreRoom" before storing any
        /// data.  This method is "Missing" not "Exists" because implementations that
        /// do not use a store room can return "false" from this method without
        /// breaking either a user's app or the spirit of the method.
        /// 
        /// Store room typically corresponds to "table".
        /// </summary>
        /// <returns>Returns true if you need to call "CreateStoreRoom"
        ///          before storing any data.</returns>
        public override bool StoreRoomMissing(ClassMapping mapping)
        {
            int count = SqlConnectionUtilities.XSafeIntQuery(_connDesc,
                "SELECT COUNT(*) FROM sqlite_master where type = 'table' and name = '" +
                mapping.Table + "'", null);
            return count == 0;
        }

        /// <summary>
        /// Returns whether a sequence with this name exists or not.
        /// </summary>
        /// <param name="name">Name of the sequence to check for.</param>
        /// <returns>Whether a sequence with this name exists in the data source.</returns>
        public override bool SequenceExists(string name)
        {
            int count = SqlConnectionUtilities.XSafeIntQuery(_connDesc,
                "SELECT COUNT(*) FROM sqlite_master where type = 'sequence' and name = '" +
                name + "'", null);
            return count == 0;
        }

        /// <summary>
        /// Returns true if you need to call "CreateStoreHouse" before storing any
        /// data.  This method is "Missing" not "Exists" because implementations that
        /// do not use a store house (I.E. single-file-based data access layers) can
        /// return "false" from this method without breaking either a user's app or the
        /// spirit of the method.
        /// 
        /// Store house typically corresponds to "database".
        /// 
        /// For SQLite, this merely verifies the file exists, not that you have
        /// any particular security permissions or even that it is a valid SQLite
        /// database.
        /// </summary>
        /// <returns>Returns true if you need to call "CreateStoreHouse"
        ///          before storing any data.</returns>
        public override bool StoreHouseMissing()
        {
            return !(File.Exists(((SQLiteDescriptor)_connDesc).DatabasePath));
        }

        /// <summary>
        /// Creates the store house specified in the connection descriptor.  If this
        /// data source doesn't use a store house, this method should be a no-op.
        /// 
        /// If this data source DOES use store houses, but support for adding
        /// them is not implemented yet, this should throw a NotImplementedException.
        /// 
        /// Store house typically corresponds to "database".
        /// 
        /// For SQLite this creates a blank database file (no tables).
        /// </summary>
        public override void CreateStoreHouse()
        {
            SQLiteConnection.CreateFile(((SQLiteDescriptor)_connDesc).DatabasePath);
        }

        /// <summary>
        /// Deletes the store house specified in the connection descriptor.  If this
        /// data source doesn't use a store house, this method should be a no-op.
        /// 
        /// If this data source DOES use store houses, but support for dropping
        /// them is not implemented yet, this should throw a NotImplementedException.
        /// 
        /// Store house typically corresponds to "database".
        /// 
        /// If there is no store house with the given name, this should be a no-op.
        /// 
        /// For SQLite, this just deletes the database file.
        /// </summary>
        public override void DeleteStoreHouse()
        {
            File.Delete(((SQLiteDescriptor)_connDesc).DatabasePath);
        }

        #endregion
    }
}