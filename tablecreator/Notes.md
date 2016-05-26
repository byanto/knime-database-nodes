# Database Table Creator

## DIALOG

### Table
* Schema is optional
* Automatically insert "newtable" for table name, even if it is empty
* Temporary table is optional
* Drop existing table is optional
	* Warning message is drop table is enabled

### Columns
* Remove and RemoveAll buttons are disabled if no rows in the table
* Column name:
	* Cannot be empty
	* No duplicate
	* Case-insensitive
	* Automatically add new name using increasing index and always use the smallest index that is available
	* Index starts from 1
	* If a column name is changed, the column name in the "Keys" tab will also be renamed
	* Remove columns will also remove all keys that are associated with those columns
		* A popup will ask for user confirmation before removing the columns
		* Popup will only be shown iff at least one key uses the columns that will be removed 
* Column type:
	* Type new column type in the field
		* If the new type already exists, the existed one will be selected
		* If it doesn't exist, it will be added to the list
		* If it is empty, "varchar(255)" will be selected
		* If the cancel button is clicked, all new type will be removed
	* Use dropdown combobox to select column type
	* If "OK" or "Apply" is clicked, all new type will be saved and can be used from other "Database Table Creator" node
	* Available types depend on the "DB Identifier". It will only show the available types for the specified "DB Identifier"
* Not Null:
	* Default value is true
* Context-Menu (right click):



### Keys
* Remove and RemoveAll buttons are disabled if no rows in the table

### Dynamic Type Settings


### Dynamic Keys Settings