# BulkInsert
Fast way to insert and update a very large list of rows in MS SQL using C#

Problem is that insert and update operations in MS SQL are very slow if you use Entity Framework or SqlCommand in the standard way. 

This repo include two examples on how to do it the fast way using SqlBulkCopy. 

One example (SomeEntity.cs) show how to add BulkInsert on an Entity Framework 
Other example (BulkSQLWriter.cs) show how to use BulkInsert and BulkUpdate using plain ADO.NET

Please note that BulkUpdate create a temp table in the database and drop it again. 

# USAGE
```
public class YourDTO
{
    [DataMember]
    public int YourTableID { get; set; }
    [DataMember]
    public string colA { get; set; }
    [DataMember]
    public string colB { get; set; }
}

// code to update database fast!
List<YourDTO> result = GetItemsToUpdate();  // return 100000 rows. 
BulkSQLWriter _sqlWriter = new BulkSQLWriter(ConfigurationManager.ConnectionStrings["MyDatabaseName"].ConnectionString);
const string sqlUpdate = "UPDATE T SET colA = Temp.colA, colB = Temp.colB FROM YourTableName T INNER JOIN #TmpTableYourTableName Temp ON T.YourTableID = Temp.YourTableID";
_sqlWriter.UpdateData(result, sqlUpdate, "YourTableName");
```
