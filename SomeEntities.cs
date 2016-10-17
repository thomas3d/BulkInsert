using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Reflection;
using System.Data.Entity;
using System.Data.Entity.Infrastructure;

namespace BulkInsert
{
    public partial class SomeEntities
    {
        public SomeEntities(int commandTimeout) : base("name=SomeEntities")
        {
            var objectContext = (this as IObjectContextAdapter).ObjectContext;
            objectContext.CommandTimeout = commandTimeout;
        }

        public int BulkInsert<T>(IEnumerable<T> list, string tableName)
        {
            using (var bulkCopy = new SqlBulkCopy(Database.Connection.ConnectionString))
            {
                TableAttribute tableAttribute = (TableAttribute)Attribute.GetCustomAttribute(typeof(T), typeof(TableAttribute));
                if (tableAttribute != null)
                    bulkCopy.DestinationTableName = tableAttribute.Name;
                else if (!string.IsNullOrWhiteSpace(tableName))
                    bulkCopy.DestinationTableName = tableName;
                else
                    throw new Exception("The Table name was not found in BulkInsert.");

                bulkCopy.BatchSize = list.Count();

                var table = new DataTable();
                var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    //Dirty hack to make sure we only have system data types 
                    //i.e. filter out the relationships/collections
                                           .Where(x => x.CanRead && x.CanWrite && x.PropertyType.Namespace.Equals("System"))
                                           .ToList();

                foreach (var x in props)
                {
                    bulkCopy.ColumnMappings.Add(x.Name, x.Name);
                    var type = Nullable.GetUnderlyingType(x.PropertyType) ?? x.PropertyType;
                    table.Columns.Add(x.Name, type);
                    // Debug.Print("[" + x.Name + "]   " + type.ToString());
                }

                var values = new object[props.Count()];
                foreach (var item in list)
                {
                    for (var i = 0; i < values.Length; i++)
                        values[i] = props[i].GetValue(item);

                    table.Rows.Add(values);
                }

                bulkCopy.BulkCopyTimeout = 200;
                bulkCopy.WriteToServer(table);

                return table.Rows.Count;
            }
        }   
    }
}
