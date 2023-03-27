package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {
    /**
     * <Unique id of a table -- TableInfo>
     */
    HashMap<Integer, TableInfo> tableInfoMap;
    /**
     * <Unique name of a table -- TableID>
     */
    HashMap<String, Integer> tableNameIdMap;

    /**
     * Helper class to store necessary info about a table,
     * Use this class a the value in Catalog
     */
    class TableInfo {
        public int tableID;
        public DbFile tableFile;
        public String tableName;
        public String pkeyField;

        TableInfo(int _tableID, DbFile _tableFile, String _tableNmae, String _pkeyField) {
            this.tableID = _tableID;
            this.tableFile = _tableFile;
            this.tableName = _tableNmae;
            this.pkeyField = _pkeyField;
        }
    }

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // TODO:some code goes here
        tableInfoMap = new HashMap<>();
        tableNameIdMap = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identifier of
     *      this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     *      conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // TODO:some code goes here
        TableInfo info = new TableInfo(file.getId(), file, name, pkeyField);
        tableInfoMap.put(file.getId(), info);
        tableNameIdMap.put(name, file.getId());
        // TODO:MIGHT NEED TO DELETE DUPLICATE TABLE
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // TODO:some code goes here
        if (!tableNameIdMap.containsKey(name))
            throw new NoSuchElementException("Table name doesn't exist");
        return tableNameIdMap.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // TODO:some code goes here
        if (!tableInfoMap.containsKey(tableid))
            throw new NoSuchElementException("Table id doesn't exist");
        return tableInfoMap.get(tableid).tableFile.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // TODO:some code goes here
        if (!tableInfoMap.containsKey(tableid))
            throw new NoSuchElementException("Table id doesn't exist");
        return tableInfoMap.get(tableid).tableFile;
    }

    public String getPrimaryKey(int tableid) {
        // TODO:some code goes here
        if (!tableInfoMap.containsKey(tableid))
            throw new NoSuchElementException("Table id doesn't exist");
        return tableInfoMap.get(tableid).pkeyField;
    }

    public Iterator<Integer> tableIdIterator() {
        // TODO:some code goes here
        return this.tableInfoMap.keySet().iterator();
    }

    public String getTableName(int tableid) {
        // TODO:some code goes here
        if (!tableInfoMap.containsKey(tableid))
            throw new NoSuchElementException("Table id doesn't exist");
        return tableInfoMap.get(tableid).tableName;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // TODO:some code goes here
        tableInfoMap.clear();
        tableNameIdMap.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

