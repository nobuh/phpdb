<?php
namespace phpdb;

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
const ROWS_PER_PAGE = (PAGE_SIZE - PAGE_SIZE % ROW_SIZE) / ROW_SIZE;
const TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

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
const LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

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

    public function print_row():void {
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
        if ($table->num_rows >= TABLE_MAX_ROWS) {
            return ExecuteResult::EXECUTE_TABLE_FULL;
        }
    
        $row_to_insert = $this->row_to_insert;
        $cursor = $table->table_end();
        $table->serialize_row($row_to_insert, $cursor->prepare_io());
        $table->num_rows += 1;

        $cursor = null;    
        return ExecuteResult::EXECUTE_SUCCESS;
    }

    public function execute_select(Table $table): ExecuteResult {   
        $cursor = $table->table_start();
        $row = new Row();
        while (!$cursor->end_of_table) {
            $table->deserialize_row($cursor->prepare_io(), $row);
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
    }

    public function get_page(int $page_num): mixed {
        if ($page_num > TABLE_MAX_PAGES) {
            echo "Tried to fetch page number out of bounds. ", $page_num, " > ", TABLE_MAX_PAGES;
            exit(EXIT_FAILURE);
        }
    
        if (!isset($this->pages[$page_num])) {
            $page = fopen("php://temp", "r+");
            $num_pages = floor($this->file_length / PAGE_SIZE);
            
            // We might save a partial page at the end of the file
            if ($this->file_length % PAGE_SIZE) {
                $num_pages += 1;
            }
            
            if ($page_num <= $num_pages) {
                fseek($this->file_descriptor, $page_num * PAGE_SIZE, SEEK_SET);
                $buffer = fread($this->file_descriptor, PAGE_SIZE);
                $bytes_read = fwrite($page, $buffer);
                if ($bytes_read === false) {
                    printf("Error reading file: %d\n", $bytes_read);
                    exit(EXIT_FAILURE);
                }
            }
            
            $this->pages[$page_num] = $page;
        }
        
        return $this->pages[$page_num];
    }

    public function flush(int $page_num, int $size): void {
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
            $buffer = fread($this->pages[$page_num], $size);
        } else {
            printf("Error seeking: %d\n",);
            exit(EXIT_FAILURE);
        }

        $bytes_written = fwrite($this->file_descriptor, $buffer, $size);
        if ($bytes_written === false) {
            printf("Error writing: %d\n", $bytes_written);
        exit(EXIT_FAILURE);
        }  
    }
}

class Table
{
    public int $num_rows;
    public Pager $pager;

    public function __construct(int $num_rows, Pager $pager) {
        $this->num_rows = $num_rows;
        $this->pager = $pager;
    }

    public function serialize_row(Row $source, int $page_num): void {
        // $row = str_pad((string)$source->id, ID_SIZE);
        $row = pack("N", $source->id);
        $row .= str_pad($source->username, USERNAME_SIZE);
        $row .= str_pad($source->email, EMAIL_SIZE);
        if (!fwrite($this->pager->pages[$page_num], $row, ROW_SIZE)) {
            echo "Error write to page ", $page_num, PHP_EOL;
            exit(EXIT_FAILURE);
        }
    }
    
    public function deserialize_row(int $page_num, Row $destination): void {
        $source = fgets($this->pager->pages[$page_num], ROW_SIZE);
        $id = substr($source, ID_OFFSET, ID_SIZE);
        $destination->id = unpack("N", $id)[1];
        $destination->username = rtrim(substr($source, USERNAME_OFFSET, USERNAME_SIZE));
        $destination->email = rtrim(substr($source, EMAIL_OFFSET, EMAIL_SIZE));
    }

    public function db_close(): void {
        $pager = $this->pager;
        $num_full_pages = floor($this->num_rows / ROWS_PER_PAGE);
        
        for ($i = 0; $i < $num_full_pages; $i++) {
            if ($pager->pages[$i] === null) {
                continne;
            }
            $pager->flush($i, PAGE_SIZE);
            $pager->pages[$i] = null;
        }
    
        $num_additional_rows = $this->num_rows % ROWS_PER_PAGE;
        if ($num_additional_rows > 0) {
            $page_num = $num_full_pages;
            if ($pager->pages[$page_num] !== null) {
                $pager->flush($page_num, $num_additional_rows * ROW_SIZE);
                $pager->pages[$page_num] = null;
            }
        }
    
        $result = fclose($pager->file_descriptor);
        if ($result === false) {
            printf("Error closing db file.\n");
            exit(EXIT_FAILURE);
        }
        $pager = null;
    }

    public function table_start(): Cursor {
        if ($this->num_rows === 0) {
            $isEnd = true;
        } else {
            $isEnd = false;
        }
        $cursor = new Cursor($this, 0, $isEnd);
        return $cursor;
    } 

    public function table_end(): Cursor {
        $cursor = new Cursor($this, $this->num_rows, true);
        return $cursor;
    }
}

Class Cursor
{
    public Table $table;
    public int $row_num;
    public bool $end_of_table;

    public function __construct(Table $table, int $row_num, bool $end_of_table) {
        $this->table = $table;
        $this->row_num = $row_num;
        $this->end_of_table = $end_of_table;
    }

    // aka cursor_value()
    public function prepare_io(): int {
        $page_num = floor($this->row_num / ROWS_PER_PAGE);
        $page = $this->table->pager->get_page($page_num);

        $row_offset = $this->row_num % ROWS_PER_PAGE;
        $byte_offset = $row_offset * ROW_SIZE;

        if (fseek($page, $byte_offset, SEEK_SET) === FSEEK_FAILED) {
            echo "fseek failed on page ", $page_num, " row ", $row_num, " row_offset ", $row_offset, " byte_offset ", $byte_offset, PHP_EOL;
            exit(EXIT_FAILURE);
        } else {
            return $page_num;
        }
    }

    public function cursor_advance() {
        $this->row_num += 1;
        if ($this->row_num >= $this->table->num_rows) {
            $this->end_of_table = true;
        }
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
        } else {
            return MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND;
        }
    }

    public function db_open(string $filename): Table {
        $pager = new Pager($filename);
        $num_rows = floor($pager->file_length / ROW_SIZE);
        $table = new Table($num_rows, $pager);
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