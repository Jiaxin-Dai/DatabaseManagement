
-------------------
CS525-2020Fall-Assignment4_GROUP#12
-------------------

-------------------
Team members:
-------------------
Jiaxin Dai/Jiangsong Wang/Sihan Yang

-------------------
running the program
-------------------
1.,cd into the file path
2,make clean
3. make
4. ./test_assign4
5. make expr
6, Checking result

------------------------------------------------
ideas behind our solution and the code structure
------------------------------------------------
here is a list of functions with description:

initIndexManager():Initialize a  storage manager

shutdownRecordManager():Shut down a Record Manager, set all global variables to NULL

createBtree():create a btree index

openBtree():Open a btree index
		
closeBtree():Close a btree index

deleteBtree():Delete a btree index
		
insertKey():Insert a key
		
findkey():Find a key in the storage

getNumNodes():Count num of nodes

getNumEntries():Count num of entries

getKeyType():get the type of key

deleteKey():delete key

openTreeScan():open scan function of a tree

nextEntry():jump to next entry

closeTreeScan():close scan function of a tree
		
printTree():print tree out

lessTo():set a rule when the key value is less to another key value

EqualOrGreaterTo:set a rule when the key value are equal or greater to another one
	
isEqual():set a rule when the key value is equal to another key value	

createNewBTree():create a new b tree if needed

insertleaf():insert leaf on a tree

insertLeafAfterSplit():operate intert a leaf after a split

insertNodeAfterSplit():operate insert a node after a split	
		
insertParent():insert parent
		
insertNode():insert a single node
		
createNode():create a node
		
searchLeaf():search for a leaf
		
searchRecord():search for a record
		
modifyRoot():modify root when needed
		
mergeNodes():operate merging nodes
		
DeleteEntry():delete entry
		
DeleteNode():delete node
		
reBalanceNodes:operate to re-balance the node