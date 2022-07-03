<?php

const EXIT_SUCCESS = 0;
const EXIT_FAILURE = 0;

const COLUMN_ID_SIZE = 19;
const COLUMN_USERNAME_SIZE = 32;
const COLUMN_EMAIL_SIZE = 255;

const ID_SIZE = COLUMN_ID_SIZE + 1;
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

const PAGE_FILE_PREFIX = "page";
const PAGE_FILE_SUFFIX = ".data";
const FSEEK_FAILED = -1;

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
}

class Row
{
	public ?int 	  $id;
	public ?string $username;
	public ?string $email;		
}

class Statement
{
	public ?StatementType $type;
	public Row $row_to_insert;

	public function __construct() {
		$this->type = null;
		$this->row_to_insert = new Row();
	}
}

class Table
{
	public int $num_rows;
	public array $pages;

	public function __construct() {
		$this->num_rows = 0;
		$this->new_page(0);
	}

	public function new_page(int $n) {
		$this->pages[$n] = fopen("php://memory", "r+");	
	}
}

function print_row(Row $row) {
	printf("(%d, %s, %s)\n", $row->id, $row->username, $row->email);
}


function print_prompt(): void {
	echo "db > ";
}

function read_input(InputBuffer $input_buffer): void {
	$input = fgets(STDIN);
	$input_buffer->buffer_length = strlen($input);

	if ($input === false) {
		echo "Error reading input", PHP_EOL;
		exit(EXIT_FAILURE);
	}

	$input_buffer->buffer = trim($input);
	$input_buffer->input_length= strlen($input_buffer->buffer);
}

function do_meta_command(InputBuffer $input_buffer): MetaCommandResult {
	if ($input_buffer->buffer === ".exit") {
		exit(EXIT_SUCCESS);
	} else {
		return MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

function prepare_insert(InputBuffer $input_buffer, Statement $statement): PrepareResult {
	$statement->type = StatementType::STATEMENT_INSERT;

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
	$statement->row_to_insert->id = $id;
	$statement->row_to_insert->username = $username;
	$statement->row_to_insert->email = $email;

	return PrepareResult::PREPARE_SUCCESS;
}

function prepare_statement(InputBuffer $input_buffer, Statement $statement): PrepareResult {
	if (substr($input_buffer->buffer, 0, 6) === "insert") {
		return prepare_insert($input_buffer, $statement);
	}
	if ($input_buffer->buffer === "select") {
		$statement->type = StatementType::STATEMENT_SELECT;
		return PrepareResult::PREPARE_SUCCESS;
	}

	return PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT;
}

function execute_insert(Statement $statement, Table $table): ExecuteResult {
	if ($table->num_rows >= TABLE_MAX_ROWS) {
		return ExecuteResult::EXECUTE_TABLE_FULL;
	}

	$row_to_insert = $statement->row_to_insert;
	serialize_row($row_to_insert, $table, row_slot($table, $table->num_rows));
	$table->num_rows += 1;

	return ExecuteResult::EXECUTE_SUCCESS;
}

function execute_select(Statement $statement, Table $table): ExecuteResult {
	$row = new Row();
	for ($i = 0; $i < $table->num_rows; $i++) {
		deserialize_row($table, row_slot($table, $i), $row);
		print_row($row);
	}
	return ExecuteResult::EXECUTE_SUCCESS;
}

function execute_statement(Statement $statement, Table $table): ExecuteResult {
	switch ($statement->type) {
		case StatementType::STATEMENT_INSERT:
			return execute_insert($statement, $table);
		case StatementType::STATEMENT_SELECT:
			return execute_select($statement, $table);
	}
}

function serialize_row(Row $source, Table $table, int $page_num): void {
	$row = "";
	$row .= str_pad((string)$source->id, ID_SIZE);
	$row .= str_pad($source->username, USERNAME_SIZE);
	$row .= str_pad($source->email, EMAIL_SIZE);
	if (!fwrite($table->pages[$page_num], $row, ROW_SIZE)) {
		echo "Error write to page ", $page_num, PHP_EOL;
		exit(EXIT_FAILURE);
	}
}

function deserialize_row(Table $table, int $page_num, Row $destination): void {
	$source = fgets($table->pages[$page_num], ROW_SIZE);
	$destination->id = (int)rtrim(substr($source, ID_OFFSET, ID_SIZE));
	$destination->username = rtrim(substr($source, USERNAME_OFFSET, USERNAME_SIZE));
	$destination->email = rtrim(substr($source, EMAIL_OFFSET, EMAIL_SIZE));
}

/**
 * row_num に応じてページファイルを選択し、seek を設定しておく
 * ページファイルの番号を返す
 * 
 * 今は状態で渡すことになるのでポインタのように渡せないか要研究
 */
function row_slot(Table $table, int $row_num): int {
	if ($row_num < 0) {
		echo "Row number must be >= 0, but ", $row_num, PHP_EOL;
		exit(EXIT_FAILURE);
	}

	$page_num = $row_num / ROWS_PER_PAGE;
	if (!isset($table->pages[$page_num])) {
		$table->new_page($page_num);
	}

	$row_offset = $row_num % ROWS_PER_PAGE;
	$byte_offset = $row_offset * ROW_SIZE;
	if (fseek($table->pages[$page_num], $byte_offset, SEEK_SET) === FSEEK_FAILED) {
		echo "fseek failed on page ", $page_num, PHP_EOL;
		exit(EXIT_FAILURE);
	} else {
		return $page_num;
	}
}

/**
 * main
 */
$table = new Table();
$input_buffer = new InputBuffer();

while (true) {
	print_prompt();
	read_input($input_buffer);

	if (substr($input_buffer->buffer, 0, 1) === '.') {
		switch (do_meta_command($input_buffer, $table)) {
			case MetaCommandResult::META_COMMAND_SUCCESS:
				continue 2;
			case MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND:
				echo "Unrecognized command ", $input_buffer->buffer, PHP_EOL;
				continue 2;
		}
	}

	$statement = new Statement();
	switch (prepare_statement($input_buffer, $statement)) {
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

	switch (execute_statement($statement, $table)) {
		case ExecuteResult::EXECUTE_SUCCESS:
			echo "Executed.", PHP_EOL;
			break;
		case ExecuteResult::EXECUTE_TABLE_FULL:
			echo "Error: Table full.", PHP_EOL;
			break;
	}
}