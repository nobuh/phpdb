<?php
namespace phpdb;

use Exception;

const EXIT_SUCCESS = 0;
const EXIT_FAILURE = 0;

const COLUMN_ID_SIZE = 4;   // 32 bit uint
const COLUMN_USERNAME_SIZE = 32;
const COLUMN_EMAIL_SIZE = 255;

const ID_SIZE = COLUMN_ID_SIZE;
const USERNAME_SIZE = COLUMN_USERNAME_SIZE + 1;
const EMAIL_SIZE = COLUMN_EMAIL_SIZE + 1;	 // +1 must be 
const ID_OFFSET = 0;
const USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE = 4096;
const TABLE_MAX_PAGES = 100;

const FSEEK_FAILED = -1;

/*
 * Common Node Header Layout
 */
const NODE_TYPE_SIZE = 1;
const NODE_TYPE_OFFSET = 0;
const IS_ROOT_SIZE = 1;
const IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const PARENT_POINTER_SIZE = 4;
const PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/*
 * Leaf Node Header Layout
 */
const LEAF_NODE_NUM_CELLS_SIZE = 4;
const LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const LEAF_NODE_NEXT_LEAF_SIZE = 4;
const LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

/*
 * Leaf Node Body Layout
 */
const LEAF_NODE_KEY_SIZE = 4;
const LEAF_NODE_KEY_OFFSET = 0;
const LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const LEAF_NODE_MAX_CELLS = (LEAF_NODE_SPACE_FOR_CELLS - LEAF_NODE_SPACE_FOR_CELLS % LEAF_NODE_CELL_SIZE) / LEAF_NODE_CELL_SIZE;

const NODE_LEAF     = 0;
const NODE_INTERNAL = 1;

const LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1 - (LEAF_NODE_MAX_CELLS + 1) % 2) / 2;
const LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/*
 * Internal Node Header Layout
 */
const INTERNAL_NODE_NUM_KEYS_SIZE = 4;
const INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const INTERNAL_NODE_RIGHT_CHILD_SIZE = 4;
const INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
 * Internal Node Body Layout
 */
const INTERNAL_NODE_KEY_SIZE = 4;
const INTERNAL_NODE_CHILD_SIZE = 4;
const INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

enum MetaCommandResult 
{
    case META_COMMAND_SUCCESS;
    case META_COMMAND_UNRECOGNIZED_COMMAND;
}

enum PrepareResult
{
    case PREPARE_SUCCESS;
    case PREPARE_NEGATIVE_ID;
    case PREPARE_STRING_TOO_LONG;
    case PREPARE_SYNTAX_ERROR;
    case PREPARE_UNRECOGNIZED_STATEMENT;
}

enum ExecuteResult
{
    case EXECUTE_SUCCESS;
    case EXECUTE_DUPLICATE_KEY;
    case EXECUTE_TABLE_FULL;
}

enum StatementType
{
    case STATEMENT_INSERT;
    case STATEMENT_SELECT;
}

class InputBuffer
{
    public ?string $buffer;
    public int	   $buffer_length;
    public int	   $input_length;

    public function __construct() {
        $this->buffer = null;
        $this->buffer_length = 0;
        $this->input_length = 0;
    }	

    public function read_input(): void {
        $input = fgets(STDIN);
        $this->buffer_length = strlen($input);
    
        if ($input === false) {
            echo "Error reading input", PHP_EOL;
            exit(EXIT_FAILURE);
        }
    
        $this->buffer = trim($input);
        $this->input_length= strlen($this->buffer);
    }
}

class Row
{
    public ?int 	  $id;
    public ?string $username;
    public ?string $email;		

    public function print_row():void 
    {
        printf("(%d, %s, %s)\n", $this->id, $this->username, $this->email);
    }
}

class Statement
{
    public ?StatementType $type;
    public Row $row_to_insert;

    public function __construct() {
        $this->type = null;
        $this->row_to_insert = new Row();
    }

    public function prepare_insert(InputBuffer $input_buffer): PrepareResult {
        $this->type = StatementType::STATEMENT_INSERT;
    
        $keyword = strtok($input_buffer->buffer, " ");
        $id_string = strtok(" ");
        $username = strtok(" ");
        $email = strtok(" ");
    
        if ($id_string === null || $username === null || $email === null) {
            return PrepareResult::PREPARE_SYNTAX_ERROR;
        }
    
        $id = (int)$id_string;
        if ($id < 0) {
            return PrepareResult::PREPARE_NEGATIVE_ID;
        }
        if (strlen($username) > COLUMN_USERNAME_SIZE) {
            return PrepareResult::PREPARE_STRING_TOO_LONG;
        }
        if (strlen($email) > COLUMN_EMAIL_SIZE) {
            return PrepareResult::PREPARE_STRING_TOO_LONG;
        }
        $this->row_to_insert->id = $id;
        $this->row_to_insert->username = $username;
        $this->row_to_insert->email = $email;
    
        return PrepareResult::PREPARE_SUCCESS;
    }

    public function prepare_statement(InputBuffer $input_buffer): PrepareResult {
        if (substr($input_buffer->buffer, 0, 6) === "insert") {
            return $this->prepare_insert($input_buffer);
        }   
        if ($input_buffer->buffer === "select") {
            $this->type = StatementType::STATEMENT_SELECT;
            return PrepareResult::PREPARE_SUCCESS;
        }
    
        return PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT;
    }

    public function execute_insert(Table $table): ExecuteResult {
        $node = $table->pager->get_page($table->root_page_num);
        $buf = fread(leaf_node_num_cells($node), LEAF_NODE_NUM_CELLS_SIZE);
        $num_cells = unpack("N", $buf)[1];
        
        $row_to_insert = $this->row_to_insert;
        $key_to_insert = $row_to_insert->id;
        $cursor = $table->table_find($key_to_insert);

        if ($cursor->cell_num < $num_cells) {
            $node = leaf_node_key($node, $cursor->cell_num);
            $key_at_index = unpack("N", fread($node, LEAF_NODE_KEY_SIZE))[1];
            fseek($node, - LEAF_NODE_KEY_SIZE, SEEK_CUR);

            if ($key_at_index === $key_to_insert) {
                return ExecuteResult::EXECUTE_DUPLICATE_KEY;
            }
        }

        leaf_node_insert($cursor, $row_to_insert->id, $row_to_insert); 

        $cursor = null;    
        return ExecuteResult::EXECUTE_SUCCESS;
    }

    public function execute_select(Table $table): ExecuteResult {   
        $cursor = $table->table_start();
        $row = new Row();
        while (!$cursor->end_of_table) {
            $table->deserialize_row($cursor->cursor_value(), $row);
            $row->print_row();  
            $cursor->cursor_advance();
        }

        $cursor = null;
        return ExecuteResult::EXECUTE_SUCCESS;
    }

    public function execute_statement(Table $table): ExecuteResult {
        switch ($this->type) {
            case StatementType::STATEMENT_INSERT:
                return $this->execute_insert($table);
            case StatementType::STATEMENT_SELECT:
                return $this->execute_select($table);
        }
    }
}

class Pager
{
    public $file_descriptor;
    public int $file_length;
    public int $num_pages;
    public ?array $pages;

    public function __construct(string $filename) {
        $fd = fopen($filename, "c+");
        if ($fd === false) {
            echo "Unable to open file", PHP_EOL;
            exit(EXIT_FAILURE);
        }
        $this->file_descriptor = $fd;
        $fstat = fstat($fd);
        $this->file_length = $fstat['size'];
        $this->num_pages = floor($this->file_length / PAGE_SIZE);

        if ($this->file_length % PAGE_SIZE !== 0) {
            printf("Db file is not a whole number of pages. Corrupt file.\n");
            exit(EXIT_FAILURE);    
        }
    }

    public function get_page(int $page_num): mixed {
        if ($page_num > TABLE_MAX_PAGES) {
            echo "Tried to fetch page number out of bounds. ", $page_num, " > ", TABLE_MAX_PAGES;
            exit(EXIT_FAILURE);
        }
    
        if (!isset($this->pages[$page_num])) {
            $page = fopen("php://temp", "r+");
            if (!ftruncate($page, PAGE_SIZE)) {
                printf("Page stream clear failed");
                exit(EXIT_FAILURE);
            }
            $num_pages = floor($this->file_length / PAGE_SIZE);
            
            // We might save a partial page at the end of the file
            if ($this->file_length % PAGE_SIZE) {
                $num_pages += 1;
            }
            
            if ($page_num < $num_pages) {
                fseek($this->file_descriptor, $page_num * PAGE_SIZE, SEEK_SET);
                $buffer = fread($this->file_descriptor, PAGE_SIZE);
                $bytes_read = fwrite($page, $buffer);
                if ($bytes_read === false) {
                    printf("Error reading file: %d\n", $bytes_read);
                    exit(EXIT_FAILURE);
                }
            }
          
            $this->pages[$page_num] = $page;

            if ($page_num >= $this->num_pages) {
                $this->num_pages = $page_num + 1;
            }
        }

        return $this->pages[$page_num];
    }

    // aka pager_flush
    public function flush(int $page_num): void 
    {
        if (!isset($this->pages[$page_num])) {
            printf("Tried to flush null page\n");
            exit(EXIT_FAILURE);
        }

        $offset = fseek($this->file_descriptor, $page_num * PAGE_SIZE);
        if ($offset === false) {
            printf("Error seeking: %d\n",);
        exit(EXIT_FAILURE);
        }

        if (rewind($this->pages[$page_num])) {
            $buffer = fread($this->pages[$page_num], PAGE_SIZE);
        } else {
            printf("Error seeking: %d\n",);
            exit(EXIT_FAILURE);
        }

        $bytes_written = fwrite($this->file_descriptor, $buffer, PAGE_SIZE);
        if ($bytes_written === false) {
            printf("Error writing: %d\n", $bytes_written);
        exit(EXIT_FAILURE);
        }  
    }
}

class Table
{
    public Pager $pager;
    public int $root_page_num;

    public function __construct(Pager $pager) {
        $this->pager = $pager;
        $this->root_page_num = 0;
    }

    public function serialize_row(Row $source, mixed $page): void {
        // $row = str_pad((string)$source->id, ID_SIZE);
        $row = pack("N", $source->id);
        $row .= str_pad($source->username, USERNAME_SIZE);
        $row .= str_pad($source->email, EMAIL_SIZE);
        if (!fwrite($page, $row, ROW_SIZE)) {
            echo "Error write to page ", $page, PHP_EOL;
            exit(EXIT_FAILURE);
        }
    }
    
    public function deserialize_row(mixed $page, Row $destination): void {
        $source = fread($page, ROW_SIZE);
        $id = substr($source, ID_OFFSET, ID_SIZE);
        $destination->id = unpack("N", $id)[1];
        $destination->username = rtrim(substr($source, USERNAME_OFFSET, USERNAME_SIZE));
        $destination->email = rtrim(substr($source, EMAIL_OFFSET, EMAIL_SIZE));
    }

    public function db_close(): void 
    {
        $pager = $this->pager;
        
        for ($i = 0; $i < $pager->num_pages; $i++) {
            if ($pager->pages[$i] === null) {
                continue;
            }
            $pager->flush($i);
            $pager->pages[$i] = null;
        }
    
        $result = fclose($pager->file_descriptor);
        if ($result === false) {
            printf("Error closing db file.\n");
            exit(EXIT_FAILURE);
        }
        $pager = null;
    }

    public function table_start(): Cursor 
    {
        $cursor = $this->table_find(0);

        $node = $this->pager->get_page($cursor->page_num);
        
        $num_cells = unpack("N", fread(leaf_node_num_cells($node), LEAF_NODE_NUM_CELLS_SIZE))[1];
        fseek($node, - LEAF_NODE_NUM_CELLS_SIZE, SEEK_CUR);

        if ($num_cells === 0) {
            $cursor->end_of_table = true;
        } else {
            $cursor->end_of_table = false;
        }
 
        return $cursor;
    } 

    /*
     * Return the position of the given key.
     * If the key is not present, return the position where it should be inserted
     */
    function table_find(int $key): Cursor
    {
        $root_page_num = $this->root_page_num;
        $root_node = $this->pager->get_page($root_page_num);

        if (get_node_type($root_node) === NODE_LEAF) {
            return leaf_node_find($this, $root_page_num, $key);
        } else {
            return internal_node_find($this, $this->root_page_num, $key);
        }
    }
}

Class Cursor
{
    public ?Table $table;
    public ?int $page_num;
    public ?int $cell_num;
    public ?bool $end_of_table;

    public function cursor_value(): mixed {
        $page_num = $this->page_num;
        $page = $this->table->pager->get_page($page_num);
        $page = leaf_node_value($page, $this->cell_num); 
        return $page;
    }

    public function cursor_advance() 
    {
        $page_num = $this->page_num;
        $node = $this->table->pager->get_page($page_num);
        $this->cell_num += 1;
        $val = fread(leaf_node_num_cells($node), LEAF_NODE_NUM_CELLS_SIZE);
        if ($this->cell_num >= unpack("N", $val)[1]) {
            /* Advance to next leaf node */
            $next_page_num = unpack("N", fread(leaf_node_next_leaf($node), LEAF_NODE_NEXT_LEAF_SIZE))[1];
            fseek($node, - LEAF_NODE_NEXT_LEAF_SIZE, SEEK_CUR);
            if ($next_page_num === 0) {
                /* This was rightmost leaf */
                $this->end_of_table = true;
            } else {
                $this->page_num = $next_page_num;
                $this->cell_num = 0;
            }
        }
    }
}

function internal_node_num_keys(mixed $node): mixed 
{
    // return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
    $result = fseek($node, INTERNAL_NODE_NUM_KEYS_OFFSET, SEEK_SET);
    if ($result === FSEEK_FAILED) {
        printf("fseek failed on stream %s\n", $node);
        exit(EXIT_FAILURE);
    }
    return $node;
}

function internal_node_right_child(mixed $node): mixed 
{
    // return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
    $result = fseek($node, INTERNAL_NODE_RIGHT_CHILD_OFFSET, SEEK_SET);
    if ($result === FSEEK_FAILED) {
        printf("fseek failed on stream %s\n", $node);
        exit(EXIT_FAILURE);
    }
    return $node;
}

function internal_node_cell(mixed $node, int $cell_num): mixed 
{
    // return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
    $offset = INTERNAL_NODE_HEADER_SIZE + $cell_num * INTERNAL_NODE_CELL_SIZE;
    $result = fseek($node, $offset, SEEK_SET);
    if ($result === FSEEK_FAILED) {
        printf("internal_node_cell fseek failed at offset %d cell_num %d\n", $offset, $cell_num);
        exit(EXIT_FAILURE);
    }
    return $node;  
}

function internal_node_child(mixed $node, int $child_num): mixed 
{
    $num_keys = unpack("N", fread(internal_node_num_keys($node), INTERNAL_NODE_NUM_KEYS_SIZE))[1];
    if ($child_num > $num_keys) {
        printf("Tried to access child_num %d > num_keys %d\n", $child_num, $num_keys);
        exit(EXIT_FAILURE);
    } else if ($child_num == $num_keys) {
        return internal_node_right_child($node);
    } else {
        return internal_node_cell($node, $child_num);
    }
}

function internal_node_key(mixed $node, int $key_num): mixed 
{
    // return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
    $seekednode = internal_node_cell($node, $key_num);
    $result = fseek($seekednode, INTERNAL_NODE_CHILD_SIZE, SEEK_CUR);
    if ($result === FSEEK_FAILED) {
        printf("fseek failed on %d\n", $seekednode);
    }
    return $seekednode;
}

function get_node_max_key(mixed $node): int 
{
    switch (get_node_type($node)) {
        case NODE_INTERNAL:
            $k = unpack("N", fread(internal_node_num_keys($node), INTERNAL_NODE_NUM_KEYS_SIZE))[1];
            fseek($node, - INTERNAL_NODE_NUM_KEYS_SIZE);
            return unpack("N", fread(internal_node_key($node, $k - 1), INTERNAL_NODE_KEY_SIZE))[1];
        case NODE_LEAF:
            $k = unpack("N", fread(leaf_node_num_cells($node), LEAF_NODE_NUM_CELLS_SIZE))[1];
            fseek($node, - LEAF_NODE_NUM_CELLS_SIZE);
            return unpack("N", fread(leaf_node_key($node, $k - 1), LEAF_NODE_KEY_SIZE))[1];
    }
}

function leaf_node_next_leaf(mixed $node): mixed 
{
    // return node + LEAF_NODE_NEXT_LEAF_OFFSET;
    $result = fseek($node, LEAF_NODE_NEXT_LEAF_OFFSET, SEEK_SET);
    if ($result === FSEEK_FAILED) {
        printf("fseek failed on stream %s\n", $node);
        exit(EXIT_FAILURE);
    }
    return $node;
}

function leaf_node_num_cells(mixed $node): mixed
{
    //  return node + LEAF_NODE_NUM_CELLS_OFFSET;
    $result = fseek($node, LEAF_NODE_NUM_CELLS_OFFSET, SEEK_SET);
    if ($result === FSEEK_FAILED) {
        printf("fseek failed on stream %s\n", $node);
        exit(EXIT_FAILURE);
    }
    return $node;
}

function leaf_node_cell(mixed $node, int $cell_num): mixed
{
    // return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
    $offset = LEAF_NODE_HEADER_SIZE + $cell_num * LEAF_NODE_CELL_SIZE;
    $result = fseek($node, $offset, SEEK_SET);
    if ($result === FSEEK_FAILED) {
        printf("leaf_node_cell fseek failed at offset %d cell_num %d\n", $offset, $cell_num);
        throw new Exception();
        exit(EXIT_FAILURE);
    }
    return $node;  
}

function get_node_type($node): int
{
    fseek($node, NODE_TYPE_OFFSET, SEEK_SET);
    $value = unpack("C", fread($node, NODE_TYPE_SIZE))[1];
    return $value;
}

function set_node_type($node, int $type): void
{
    fseek($node, NODE_TYPE_OFFSET, SEEK_SET);
    $written = fwrite($node, pack("C", $type), NODE_TYPE_SIZE);
    if ($written === false) {
        printf("Error writing: %d\n", $written);
        exit(EXIT_FAILURE);
    }
}

function is_node_root(mixed $node): bool 
{
    fseek($node, IS_ROOT_OFFSET, SEEK_SET);
    $value = unpack("C", fread($node, IS_ROOT_SIZE))[1];
    return (bool)$value;
}

function set_node_root($node, bool $is_root): void
{
    fseek($node, IS_ROOT_OFFSET, SEEK_SET);
    $written = fwrite($node, pack("C", (int)$is_root));
    if ($written === false) {
        printf("Error writing: %d\n", $written);
        exit(EXIT_FAILURE);
    }

    if (is_node_root($node) !== $is_root) {
        printf("set_node_root failed\n");
        exit(EXIT_FAILURE);
    }
}

function leaf_node_key(mixed $node, int $cell_num): mixed
{
    return leaf_node_cell($node, $cell_num);
}

function leaf_node_value(mixed $node, int $cell_num): mixed 
{
    // return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
    $seekednode = leaf_node_cell($node, $cell_num);
    $result = fseek($seekednode, LEAF_NODE_KEY_SIZE, SEEK_CUR);
    if ($result === FSEEK_FAILED) {
        printf("fseek failed on %d\n", $seekednode);
    }
    return $seekednode;
}

function initialize_leaf_node($node): void
{
    set_node_type($node, NODE_LEAF);
    set_node_root($node, false);

    $node = leaf_node_num_cells($node);
    $data = pack("N", 0);
    $written = fwrite($node, $data, LEAF_NODE_NUM_CELLS_SIZE);
    if ($written === false) {
        printf("Error writing: %d\n", $written);
        exit(EXIT_FAILURE);
    }

    $node = leaf_node_next_leaf($node);
    $written = fwrite($node, $data, LEAF_NODE_NEXT_LEAF_SIZE);
    if ($written === false) {
        printf("Error writing: %d\n", $written);
        exit(EXIT_FAILURE);
    }
}

function initialize_internal_node(mixed $node) 
{
    set_node_type($node, NODE_INTERNAL);
    set_node_root($node, false);
    $node = internal_node_num_keys($node);
    $data = pack("N", 0);
    $written = fwrite($node, $data, INTERNAL_NODE_NUM_KEYS_SIZE);
    if ($written === false) {
        printf("Error writing: %d\n", $written);
        exit(EXIT_FAILURE);
    }
}

function leaf_node_find(Table $table, int $page_num, int $key): Cursor
{
    $node = $table->pager->get_page($page_num);
    $node = leaf_node_num_cells($node);
    $num_cells = unpack("N", fread($node, LEAF_NODE_NUM_CELLS_SIZE))[1];
    fseek($node, - LEAF_NODE_NUM_CELLS_SIZE, SEEK_CUR);

    $cursor = new Cursor();
    $cursor->table = $table;
    $cursor->page_num = $page_num;

    // Binary search
    $min_index = 0;
    $one_past_max_index = $num_cells;
    while ($one_past_max_index > $min_index) {
        $index = floor(($min_index + $one_past_max_index) / 2);

        $node = leaf_node_key($node, $index);
        $key_at_index = unpack("N", fread($node, LEAF_NODE_KEY_SIZE))[1];
        fseek($node, - LEAF_NODE_KEY_SIZE, SEEK_CUR);
        if ($key === $key_at_index) {
            $cursor->cell_num = $index;
            return $cursor;
        }
        if ($key < $key_at_index) {
            $one_past_max_index = $index;
        } else {
            $min_index = $index + 1;
        }
    }

    $cursor->cell_num = $min_index;
    return $cursor;
}

function leaf_node_insert(Cursor $cursor, int $key, Row $value): void
{
    $node = $cursor->table->pager->get_page($cursor->page_num);

    $buf = fread(leaf_node_num_cells($node), LEAF_NODE_NUM_CELLS_SIZE);    
    $num_cells = unpack("N", $buf)[1];

    if ($num_cells >= LEAF_NODE_MAX_CELLS) {
        leaf_node_split_and_insert($cursor, $key, $value);
        return;
    }

    if ($cursor->cell_num < $num_cells) {
        // Make room for num cell
        for ($i = $num_cells; $i > $cursor->cell_num; $i--) {
            $buf = fread(leaf_node_cell($node, $i - 1), LEAF_NODE_CELL_SIZE);
            fwrite(leaf_node_cell($node, $i), $buf, LEAF_NODE_CELL_SIZE);
        }
    }

    // *(leaf_node_num_cells(node)) += 1
    $buf = fread(leaf_node_num_cells($node), LEAF_NODE_NUM_CELLS_SIZE);
    $num_cells = unpack("N", $buf)[1];
    $num_cells++;
    fseek($node, - LEAF_NODE_NUM_CELLS_SIZE, SEEK_CUR);
    fwrite($node, pack("N", $num_cells), LEAF_NODE_NUM_CELLS_SIZE);
    
    // *(leaf_node_key(node, cursor->cell_num)) = key
    $node = leaf_node_key($node, $cursor->cell_num);
    fwrite($node, pack("N", $key), LEAF_NODE_KEY_SIZE);

    $cursor->table->serialize_row($value, leaf_node_value($node, $cursor->cell_num)); 
}

function leaf_node_split_and_insert(Cursor $cursor, int $key, Row $value) 
{
    /*
     * Create a new node and move half the cells over.
     * Insert the new value in one of the two nodes.
     * Update parent or create a new parent.
     */
    $old_node = $cursor->table->pager->get_page($cursor->page_num);
    $new_page_num = get_unused_page_num($cursor->table->pager);
    $new_node = $cursor->table->pager->get_page($new_page_num);
    initialize_leaf_node($new_node);

    // *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    $old_node = leaf_node_next_leaf($old_node);
    $bytes = fwrite(leaf_node_next_leaf($new_node), fread($old_node, LEAF_NODE_NEXT_LEAF_SIZE));
    if ($bytes === false) {
        throw new Exception();
    }

    // *leaf_node_next_leaf(old_node) = new_page_num;
    $bytes = fwrite(leaf_node_next_leaf($old_node), pack("N", $new_page_num));
    if ($bytes === false) {
        throw new Exception();
    }

    /*
     * All existing keys plus new key should be divided
     * evenly between old (left) and new (right) nodes.
     * Starting from the right, move each key to correct position.
     */
    for ($i = LEAF_NODE_MAX_CELLS; $i >= 0; $i--) {
        if ($i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            $destination_node = $new_node;
        } else {
            $destination_node = $old_node;
        }
        $index_within_node = $i % LEAF_NODE_LEFT_SPLIT_COUNT;

        $destination = leaf_node_cell($destination_node, $index_within_node);

        if ($i === $cursor->cell_num) {
            $cursor->table->serialize_row($value, leaf_node_value($destination_node, $index_within_node));
            $bytes = fwrite(leaf_node_key($destination_node, $index_within_node), pack("N", $key));
            if ($bytes === false) {
                throw new Exception();
            }
        } else if ($i > $cursor->cell_num) {
            // memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
            $buf = fread(leaf_node_cell($old_node, $i - 1), LEAF_NODE_CELL_SIZE);
            fseek($old_node, - LEAF_NODE_CELL_SIZE);
            fwrite($destination, $buf, LEAF_NODE_CELL_SIZE);
        } else {
            // memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
            $buf = fread(leaf_node_cell($old_node, $i), LEAF_NODE_CELL_SIZE);
            fseek($old_node, - LEAF_NODE_CELL_SIZE);
            fwrite($destination, $buf, LEAF_NODE_CELL_SIZE);
        }
    }

    /* Update cell count on both leaf nodes */
    // *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    // *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;
    $buf = pack("N", LEAF_NODE_LEFT_SPLIT_COUNT);
    fwrite(leaf_node_num_cells($old_node), $buf, LEAF_NODE_NUM_CELLS_SIZE);
    $buf = pack("N", LEAF_NODE_RIGHT_SPLIT_COUNT);
    fwrite(leaf_node_num_cells($new_node), $buf, LEAF_NODE_NUM_CELLS_SIZE);
    
    if (is_node_root($old_node)) {
        return create_new_root($cursor->table, $new_page_num);
    } else {
        printf("Need to implement updating parent after split\n");
        exit(EXIT_FAILURE);
    }
}

function internal_node_find(Table $table, int $page_num, int $key): Cursor
{
    $node = $table->pager->get_page($page_num);

    $num_keys = unpack("N", fread(internal_node_num_keys($node), INTERNAL_NODE_NUM_KEYS_SIZE))[1];
    fseek($node, - INTERNAL_NODE_NUM_KEYS_SIZE, SEEK_CUR);

    /* Binary search to find index of child to search */
    $min_index = 0;
    $max_index = $num_keys; /* there is one more child than key */

    while ($min_index < $max_index) {
        $index = floor(($min_index + $max_index) / 2);

        $key_to_right = unpack("N", fread(internal_node_key($node, $index), INTERNAL_NODE_KEY_SIZE))[1];
        fseek($node, - INTERNAL_NODE_KEY_SIZE, SEEK_CUR);
        
        if ($key_to_right >= $key) {
            $max_index = $index;
        } else {
            $min_index = $index + 1;
        }
    }

    $child_num = unpack("N", fread(internal_node_child($node, $min_index), INTERNAL_NODE_CHILD_SIZE))[1];
    fseek($node, - INTERNAL_NODE_CHILD_SIZE, SEEK_CUR);

    $child = $table->pager->get_page($child_num);
    switch (get_node_type($child)) {
    case NODE_LEAF:
        return leaf_node_find($table, $child_num, $key);
    case NODE_INTERNAL:
        return internal_node_find($table, $child_num, $key);
    }
}

function create_new_root(Table $table, int $right_child_page_num) 
{
    /*
     * Handle splitting the root.
     * Old root copied to new page, becomes left child.
     * Address of right child passed in.
     * Re-initialize root page to contain the new root node.
     * New root node points to two children.
     */
    $root = $table->pager->get_page($table->root_page_num);
    //$right_child = $table->pager->get_page($right_child_page_num);
    $left_child_page_num = get_unused_page_num($table->pager);
    $left_child = $table->pager->get_page($left_child_page_num);

    /* Left child has data copied from old root */
    fseek($root, 0, SEEK_SET);
    $buf = fread($root, PAGE_SIZE);
    fseek($left_child, 0, SEEK_SET);
    fwrite($left_child, $buf, PAGE_SIZE);
    set_node_root($left_child, false);

    /* Root node is a new internal node with one key and two children */
    initialize_internal_node($root);
    set_node_root($root, true);
    $bytes = fwrite(internal_node_num_keys($root), pack("N", 1), INTERNAL_NODE_NUM_KEYS_SIZE);
    if ($bytes === false) {
        printf("fwrite failed\n");
        exit(EXIT_FAILURE);
    }
    $bytes = fwrite(internal_node_child($root, 0), pack("N", $left_child_page_num), INTERNAL_NODE_CHILD_SIZE);
    if ($bytes === false) {
        printf("fwrite failed\n");
        exit(EXIT_FAILURE);
    }
    $left_child_max_key = get_node_max_key($left_child);
    $bytes = fwrite(internal_node_key($root, 0), pack("N", $left_child_max_key), INTERNAL_NODE_KEY_SIZE);
    if ($bytes === false) {
        printf("fwrite failed\n");
        exit(EXIT_FAILURE);
    }
    $bytes = fwrite(internal_node_right_child($root), pack("N", $right_child_page_num), INTERNAL_NODE_RIGHT_CHILD_SIZE);
    if ($bytes === false) {
        printf("fwrite failed\n");
        exit(EXIT_FAILURE);
    }
}

/*
 * Until we start recycling free pages, new pages will always
 * go onto the end of the database file
 */
function get_unused_page_num(Pager $pager)
{ 
    return $pager->num_pages;
}

function print_constants(): void 
{
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

function print_leaf_node(mixed $node):void 
{
    $node = leaf_node_num_cells($node);
    $num_cells = unpack("N", fread($node, LEAF_NODE_NUM_CELLS_SIZE))[1];
    fseek($node, - LEAF_NODE_NUM_CELLS_SIZE, SEEK_CUR);
    printf("leaf (size %d)\n", $num_cells);
    for ($i = 0; $i < $num_cells; $i++) {
        $node = leaf_node_key($node, $i);
        $key = unpack("N", fread($node, LEAF_NODE_KEY_SIZE))[1];
        fseek($node, - LEAF_NODE_KEY_SIZE, SEEK_CUR);
        printf("  - %d : %d\n", $i, $key);
    }
}

function indent(int $level) 
{
    for ($i = 0; $i < $level; $i++) {
        printf("  ");
    }
}

function print_tree(Pager $pager, int $page_num, int $indentation_level) 
{
    $node = $pager->get_page($page_num);

    switch (get_node_type($node)) {
        case (NODE_LEAF):
            $num_keys = unpack("N", fread(leaf_node_num_cells($node), LEAF_NODE_NUM_CELLS_SIZE))[1];
            indent($indentation_level);
            printf("- leaf (size %d)\n", $num_keys);
            for ($i = 0; $i < $num_keys; $i++) {
                indent($indentation_level + 1);
                printf("- %d\n", unpack("N", fread(leaf_node_key($node, $i), LEAF_NODE_KEY_SIZE))[1]);
            }
            break;
        case (NODE_INTERNAL):
            $num_keys = unpack("N", fread(internal_node_num_keys($node), INTERNAL_NODE_NUM_KEYS_SIZE))[1];
            indent($indentation_level);
            printf("- internal (size %d)\n", $num_keys);
            for ($i = 0; $i < $num_keys; $i++) {
                $child = unpack("N", fread(internal_node_child($node, $i), INTERNAL_NODE_CHILD_SIZE))[1];
                fseek($node, - INTERNAL_NODE_CHILD_SIZE);
                print_tree($pager, $child, $indentation_level + 1);
                indent($indentation_level + 1);
                printf("- key %d\n", unpack("N", fread(internal_node_key($node, $i), INTERNAL_NODE_KEY_SIZE))[1]);
                fseek($node, - INTERNAL_NODE_KEY_SIZE);
            }
            $child = unpack("N", fread(internal_node_right_child($node), INTERNAL_NODE_RIGHT_CHILD_SIZE))[1];
            fseek($node, - INTERNAL_NODE_RIGHT_CHILD_SIZE);
            print_tree($pager, $child, $indentation_level + 1);
            break;
        }
}

Class Main 
{
    public function print_prompt(): void {
        echo "db > ";
    }

    public function do_meta_command(InputBuffer $input_buffer, Table $table): MetaCommandResult {
        if ($input_buffer->buffer === ".exit") {
            $table->db_close();
            exit(EXIT_SUCCESS);
        } else if ($input_buffer->buffer === ".constants") {
            printf("Constants:\n");
            print_constants();
            return MetaCommandResult::META_COMMAND_SUCCESS;
        } else if ($input_buffer->buffer === ".btree") {
            printf("Tree:\n");
            print_tree($table->pager, 0, 0);
            return MetaCommandResult::META_COMMAND_SUCCESS;
        } else {
            return MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND;
        }
    }

    public function db_open(string $filename): Table {
        $pager = new Pager($filename);
        $table = new Table($pager);

        if ($pager->num_pages === 0) {
            // New database file. Initialize page 0 as leaf node.
            $root_node = $pager->get_page(0);
            initialize_leaf_node($root_node);
            set_node_root($root_node, true);
        }
        return $table;
    }

    public function run(string $filename):void {
        $table = $this->db_open($filename);
        $input_buffer = new InputBuffer();
    
        while (true) {
            $this->print_prompt();
            $input_buffer->read_input();
    
            if (substr($input_buffer->buffer, 0, 1) === '.') {
                switch ($this->do_meta_command($input_buffer, $table)) {
                    case MetaCommandResult::META_COMMAND_SUCCESS:
                        continue 2;
                    case MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND:
                        echo "Unrecognized command ", $input_buffer->buffer, PHP_EOL;
                        continue 2;
                }
            }
    
            $statement = new Statement();
            switch ($statement->prepare_statement($input_buffer)) {
                case PrepareResult::PREPARE_SUCCESS:
                    break;
                case PrepareResult::PREPARE_NEGATIVE_ID:
                    echo "ID must be positive.", PHP_EOL;
                    continue 2;
                case PrepareResult::PREPARE_STRING_TOO_LONG:
                    echo "String is too long.", PHP_EOL;
                    continue 2;
                case PrepareResult::PREPARE_SYNTAX_ERROR:
                    echo "Syntax error. Could not parse statment.", PHP_EOL;
                    continue 2;
                case PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT:
                    echo "Unrecognized keyword at start of ", $input_buffer->buffer, PHP_EOL;
                    continue 2;
            }
    
            switch ($statement->execute_statement($table)) {
                case ExecuteResult::EXECUTE_SUCCESS:
                    echo "Executed.", PHP_EOL;
                    break;
                case ExecuteResult::EXECUTE_DUPLICATE_KEY:
                    printf("Error: Duplicate key.\n");
                    break;
                case ExecuteResult::EXECUTE_TABLE_FULL:
                    echo "Error: Table full.", PHP_EOL;
                    break;
            }
        }
    }
}

if ($argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
}
$filename = $argv[1];
$app = new Main();
$app->run($filename);