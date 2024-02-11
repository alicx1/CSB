package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	dbUsername = "user"
	dbPassword = "password"
	dbHost     = "localhost"
	dbPort     = "3306"
	dbName     = "benchmark"
)

// User structure
type User struct {
	ID          int
	FirstName   string
	LastName    string
	Age         int
	Birthday    time.Time
	Email       string
	Phone       string
	NameOfDad   string
	NameOfMom   string
	NumSiblings int
	NumChildren int
	Status      string
	Job         string
	Salary      float64
	Address     Address
}

// Address structure
type Address struct {
	Street  string
	Number  int
	ZipCode string
	Country string
}

// Global lists for diversity
var (
	firstNames = []string{"John", "Alice", "Bob", "Mary", "David", "Chris", "Jennifer", "Michael", "Emily", "James", "Elizabeth", "William", "Olivia", "Alexander", "Isabella", "Daniel", "Emma", "Matthew", "Madison", "Ethan", "Abigail", "Christopher", "Ava", "Liam", "Sophia", "Noah", "Emily", "Lucas", "Emma", "Oliver", "Olivia", "Elijah", "Ava", "William", "Sophia", "James", "Isabella", "Benjamin", "Charlotte", "Mason", "Amelia", "Ethan", "Mia", "Alexander", "Evelyn", "Michael", "Abigail", "Daniel", "Harper", "Henry", "Emily", "Jacob", "Elizabeth", "Samuel", "Madison", "Sebastian", "Ella", "Joseph", "Chloe", "Carter", "Victoria", "Jackson", "Avery", "Aiden", "Sofia", "Graham", "Camila", "Logan", "Aria", "Luke", "Scarlett", "Owen", "Grace", "Isaac", "Lily", "Jack", "Zoey", "Ryan", "Layla", "Jaxon", "Lillian", "Levi", "Nora", "Gabriel", "Aubrey", "Julian", "Hannah", "Matthew", "Addison", "Connor", "Mila", "Jayden", "Leah", "Muhammad", "Savannah", "Adam", "Stella", "Lincoln", "Paisley", "Jace", "Audrey", "Aaron", "Skylar", "Isaiah", "Violet", "Thomas", "Claire", "Charles", "Bella", "Caleb", "Aurora", "Josiah", "Lucy", "Christian", "Piper", "Christopher", "Genesis", "Andrew", "Cali", "Theodore", "Kinsley", "Joshua", "Naomi", "Nicholas", "Aaliyah", "David", "Madelyn", "Adrian", "Alexa", "Luis", "Nevaeh", "Hunter", "Elena", "Jonathan", "Gabriella", "Cameron", "Kaylee", "John", "Peyton", "Ryder", "Evelyn", "Jordan", "Sarah", "Colton", "Quinn", "Austin", "Clara"}
	lastNames  = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzales", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson", "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long", "Ross", "Foster", "Jimenez", "Powell", "Jenkins", "Perry", "Russell", "Sullivan", "Bell", "Coleman", "Butler", "Henderson", "Barnes", "Gonzales", "Fisher", "Simmons", "Richards", "Williamson", "Johnston", "Ray", "Jordan", "Reynolds", "Hamilton", "Graham", "Kim", "Griffin", "Hunter", "Hoffman", "Carlson", "Ferguson", "Simpson", "George", "Burton", "Harvey", "Little", "Burke", "Banks", "Meyer", "Bishop", "McCoy", "Howell", "Alvarez", "Morrison", "Hansen", "Fernandez", "Garza", "Harper", "Bradley", "Dunlap", "Ramsey", "Wolfe", "Schmidt", "Carr", "Vasquez", "Castaneda", "Wheeler", "Chapman", "Oliver", "Montgomery", "Richards", "Williamson", "Johnston", "Banks", "Meyer", "Bishop", "McCoy", "Howell", "Alvarez", "Morrison", "Hansen", "Fernandez", "Garza", "Harper", "Bradley", "Dunlap", "Ramsey", "Wolfe", "Schmidt", "Carr", "Vasquez", "Castaneda", "Wheeler", "Chapman", "Oliver", "Montgomery", "Richards"}
	countries  = []string{"Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua and Barbuda", "Argentina", "Armenia", "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi", "Cabo Verde", "Cambodia", "Cameroon", "Canada", "Central African Republic (CAR)", "Chad", "Chile", "China", "Colombia", "Comoros", "Congo, Democratic Republic of the", "Congo, Republic of the", "Costa Rica", "Cote d'Ivoire", "Croatia", "Cuba", "Cyprus", "Czechia", "Denmark", "Djibouti", "Dominica", "Dominican Republic", "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia", "Fiji", "Finland", "France", "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Honduras", "Hungary", "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Italy", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kosovo", "Kuwait", "Kyrgyzstan", "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Mauritania", "Mauritius", "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria", "North Korea", "North Macedonia", "Norway", "Oman", "Pakistan", "Palau", "Palestine", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Poland", "Portugal", "Qatar", "Romania", "Russia", "Rwanda", "Saint Kitts and Nevis", "Saint Lucia", "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia"}
	jobTitles  = []string{"Software Engineer", "Teacher", "Doctor", "Lawyer", "Accountant", "Project Manager", "Business Analyst", "Financial Analyst", "Marketing Manager", "Sales Manager", "Product Manager", "Human Resources", "Customer Service", "System Admin", "Web Developer", "Data Scientist", "Database Admin", "Network Engineer", "Security Engineer", "Software Architect", "UX Designer", "Business Intelligence", "Data Analyst"}
)
var csvMutex sync.Mutex

var workerPool chan bool

const maxWorkers = 10 // Adjust this value based on your system resources

func init() {
	workerPool = make(chan bool, maxWorkers)
	for i := 0; i < maxWorkers; i++ {
		workerPool <- true
	}
}

func main() {
	// Set up the database connection
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", dbUsername, dbPassword, dbHost, dbPort, dbName))
	if err != nil {
		fmt.Println(err)
	}
	defer db.Close()

	// Create tables if they don't exist
	createTables(db)
	// Create CSV file
	csvFile, err := os.Create("load_generator_results.csv")
	if err != nil {
		fmt.Println("Error creating CSV file:", err)
	}
	defer csvFile.Close()

	// Create CSV writer with a larger buffer size
	csvWriter := csv.NewWriter(bufio.NewWriterSize(csvFile, 81920)) // Adjust buffer size as needed
	defer csvWriter.Flush()

	// Write CSV header
	csvWriter.Write([]string{"ID", "RequestType", "Complexity", "TimeSent", "TimeReceived", "Duration"})

	// Load generator parameters
	numThreads := 4
	numDataPoints := 10000
	complexities := []int{1, 2, 3}

	// Use a channel to synchronize threads
	threadDone := make(chan bool)

	// Start 4 threads
	for i := 0; i < numThreads; i++ {
		go func(threadID int) {
			defer func() {
				fmt.Printf("\nthread : %+v completed", threadID)
				// Notify that the thread is done
				threadDone <- true
			}()
			fmt.Printf("\nthread %+v started pumping 1 million records", threadID)
			// Fill database with 5 million dummy data
			generateAndInsertData(db, numDataPoints)

			// Execute read and write requests concurrently for each complexity
			for _, complexity := range complexities {

				fmt.Printf("\nthread : %+v complexity : %+v read started", threadID, complexity)
				// Read request
				executeRequests(db, csvWriter, numDataPoints, complexity, "read")
				fmt.Printf("\nthread : %+v complexity : %+v read finished", threadID, complexity)

				fmt.Printf("\nthread : %+v complexity : %+v write started", threadID, complexity)
				// Write request
				executeRequests(db, csvWriter, numDataPoints, complexity, "write")
				fmt.Printf("\nthread : %+v complexity : %+v write finished", threadID, complexity)

			}
		}(i)
	}

	// Wait for all threads to finish
	for i := 0; i < numThreads; i++ {
		<-threadDone
	}

}

// Function to create tables if they don't exist
func createTables(db *sql.DB) {
	// Create Address table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS address (
			ID INT AUTO_INCREMENT PRIMARY KEY,
			Street VARCHAR(255),
			Number INT,
			ZipCode VARCHAR(20),
			Country VARCHAR(255)
		)
	`)

	if err != nil {
		fmt.Println("Error creating Address table:", err)
	}

	// Create User table with foreign key reference to Address
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS user (
			ID INT AUTO_INCREMENT PRIMARY KEY,
    		FirstName VARCHAR(255),
	LastName VARCHAR(255),
    Age INT,
    Birthday DATE, -- or DATETIME
    Email VARCHAR(255),
    Phone VARCHAR(20),
    NameOfDad VARCHAR(255),
    NameOfMom VARCHAR(255),
    NumSiblings INT,
    NumChildren INT,
    Status VARCHAR(20),
    Job VARCHAR(255),
    Salary DECIMAL(10,2),
    AddressID INT,
    FOREIGN KEY (AddressID) REFERENCES address(ID)
		)
	`)

	if err != nil {
		fmt.Println("Error creating User table:", err)
	}
}

// Function to generate and insert dummy data into the database
func generateAndInsertData(db *sql.DB, numDataPoints int) {
	// Insert dummy data into the User and Address tables
	for i := 0; i < numDataPoints; i++ {
		user := generateRandomUser()
		addressID := insertAddress(db, user.Address)
		insertUser(db, user, addressID)
	}
}

// Function to generate random User data
func generateRandomUser() User {
	return User{
		FirstName:   firstNames[rand.Intn(len(firstNames))],
		LastName:    lastNames[rand.Intn(len(lastNames))],
		Age:         rand.Intn(100),
		Birthday:    time.Now().AddDate(-rand.Intn(50), -rand.Intn(12), -rand.Intn(30)),
		Email:       fmt.Sprintf("john.doe%d@example.com", rand.Intn(100000)),
		Phone:       fmt.Sprintf("+1-%d", rand.Intn(1000000000)),
		NameOfDad:   "Dad Doe",
		NameOfMom:   "Mom Doe",
		NumSiblings: rand.Intn(10),
		NumChildren: rand.Intn(5),
		Status:      "Married",
		Job:         jobTitles[rand.Intn(len(jobTitles))],
		Salary:      rand.Float64() * 100000,
		Address:     generateRandomAddress(),
	}
}

// Function to generate random Address data
func generateRandomAddress() Address {
	return Address{
		Street:  "123 Main St",
		Number:  rand.Intn(100),
		ZipCode: fmt.Sprintf("%05d", rand.Intn(100000)),
		Country: countries[rand.Intn(len(countries))],
	}
}

// Function to insert an Address into the database and return the generated ID
func insertAddress(db *sql.DB, address Address) int {
	result, err := db.Exec("INSERT INTO address (Street, Number, ZipCode, Country) VALUES (?, ?, ?, ?)",
		address.Street, address.Number, address.ZipCode, address.Country)
	if err != nil {
		fmt.Println("Error inserting address:", err)
	}
	id, _ := result.LastInsertId()
	return int(id)
}

// Function to insert a User into the database
func insertUser(db *sql.DB, user User, addressID int) {
	_, err := db.Exec(`
		INSERT INTO user (FirstName, LastName, Age, Birthday, Email, Phone, NameOfDad, NameOfMom, NumSiblings, NumChildren, Status, Job, Salary, AddressID)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, user.FirstName, user.LastName, user.Age, user.Birthday, user.Email, user.Phone, user.NameOfDad, user.NameOfMom, user.NumSiblings, user.NumChildren, user.Status, user.Job, user.Salary, addressID)

	if err != nil {
		fmt.Println("Error inserting user:", err)
	}
}

// Function to execute read and write requests
func executeRequests(db *sql.DB, csvWriter *csv.Writer, numDataPoints, complexity int, requestType string) {
	startTimeLoop := time.Now()

	for i := 1; time.Since(startTimeLoop) <= 10*time.Minute; i++ {
		// Execute write or read request based on complexity and request type
		<-workerPool
		startTime := time.Now()

		if requestType == "write" {
			// Write request
			// ... Perform the SQL INSERT operation

			switch complexity {
			case 1:
				insertUserComplexity1(db, generateRandomUser(), rand.Intn(numDataPoints)+1)
			case 2:
				updateUserComplexity2(db)
			case 3:
				updateUserComplexity3(db)
			default:
				fmt.Printf("Unsupported complexity level: %d", complexity)
			}
		} else {
			// Read request
			// ... Perform the SQL SELECT operation
			userID := rand.Intn(numDataPoints) + 1

			switch complexity {
			case 1:
				readUserComplexity1(db)
			case 2:
				readUserComplexity2(db, userID)
			case 3:
				readUserComplexity3(db, userID)
			default:
				fmt.Printf("Unsupported complexity level: %d", complexity)
			}
		}

		// Generate a unique request ID using a combination of timestamp and randomness
		requestID := fmt.Sprintf("%d_%d", time.Now().UnixNano(), rand.Intn(100000))

		// Lock the mutex to ensure exclusive access to the CSV writer
		csvMutex.Lock()
		// Write data to CSV file with the unique request ID for every single request
		csvWriter.Write([]string{
			requestID,
			requestType,
			fmt.Sprintf("%d", complexity),
			startTime.Format(time.RFC3339Nano),
			time.Now().Format(time.RFC3339Nano),
			time.Since(startTime).String(),
		})
		// Flush the CSV writer after writing each request
		csvWriter.Flush()

		// Unlock the mutex to allow other threads to access the CSV writer
		csvMutex.Unlock()
		workerPool <- true
	}
	// Flush the CSV writer after the loop
	csvWriter.Flush()
}

// Function to read the first 5 rows from the User table
func readUserComplexity1(db *sql.DB) {
	rows, err := db.Query(`
		SELECT ID, FirstName
		FROM user
		LIMIT 5
	`)

	if err != nil {
		fmt.Println("Error reading user:", err)
	}
	defer rows.Close()

	var id int
	var firstName string

	for rows.Next() {
		err := rows.Scan(&id, &firstName)
		if err != nil {
			fmt.Println(err)
		}
	}

	if err := rows.Err(); err != nil {
		fmt.Println("Error iterating over user rows:", err)
	}
}

// Additional processing for read operation in Complexity 2
func readUserComplexity2(db *sql.DB, userID int) {
	var user User
	var birthdayStr string

	err := db.QueryRow(`
		SELECT ID, FirstName, LastName, Age, Birthday, Email, Phone, NameOfDad, NameOfMom, NumSiblings, NumChildren, Status, Job, Salary
		FROM user
		WHERE ID = ?
	`, userID).Scan(
		&user.ID, &user.FirstName, &user.LastName, &user.Age, &birthdayStr, &user.Email, &user.Phone,
		&user.NameOfDad, &user.NameOfMom, &user.NumSiblings, &user.NumChildren, &user.Status, &user.Job, &user.Salary,
	)

	if err != nil {
		fmt.Println("Error reading user (Complexity 2):", err)
	}

	user.Birthday, err = time.Parse("2006-01-02", birthdayStr)
	if err != nil {
		fmt.Println("Error parsing Birthday (Complexity 2):", err)
	}
}

// Function to read a User and corresponding Address from the database
func readUserComplexity3(db *sql.DB, userID int) {
	var user User
	var address Address
	var birthdayStr string // Temporary variable to store the string representation

	err := db.QueryRow(`
		SELECT
			u.ID, u.FirstName, u.LastName, u.Age, u.Birthday, u.Email, u.Phone,
			u.NameOfDad, u.NameOfMom, u.NumSiblings, u.NumChildren, u.Status, u.Job, u.Salary,
			a.Street, a.Number, a.ZipCode, a.Country
		FROM
			user u
		JOIN
			address a ON u.AddressID = a.ID
		WHERE
			u.ID = ?
	`, userID).Scan(
		&user.ID, &user.FirstName, &user.LastName, &user.Age, &birthdayStr, &user.Email, &user.Phone,
		&user.NameOfDad, &user.NameOfMom, &user.NumSiblings, &user.NumChildren, &user.Status, &user.Job, &user.Salary,
		&address.Street, &address.Number, &address.ZipCode, &address.Country,
	)

	if err != nil {
		fmt.Println("Error reading user with address:", err)
	}

	// Parse the "Birthday" string into a time.Time value
	user.Birthday, err = time.Parse("2006-01-02", birthdayStr)
	if err != nil {
		fmt.Println("Error parsing Birthday:", err)
	}
}

// Function to insert a User into the database with additional processing for Complexity 2
func insertUserComplexity1(db *sql.DB, user User, addressID int) {
	// Perform the SQL INSERT operation
	_, err := db.Exec(`
		INSERT INTO user (FirstName, LastName, Age, Birthday, Email, Phone, NameOfDad, NameOfMom, NumSiblings, NumChildren, Status, Job, Salary, AddressID)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, user.FirstName, user.LastName, user.Age, user.Birthday, user.Email, user.Phone, user.NameOfDad, user.NameOfMom, user.NumSiblings, user.NumChildren, user.Status, user.Job, user.Salary, addressID)

	if err != nil {
		fmt.Println("Error inserting user (Complexity 2):", err)
	}
}

// Additional processing for write operation in Complexity 2
func updateUserComplexity2(db *sql.DB) {
	// Generate random values for the update
	updatedUser := generateRandomUser()

	// Perform the SQL UPDATE operation on the first record
	_, err := db.Exec(`
		UPDATE user
		SET FirstName=?, LastName=?, Age=?, Birthday=?, Email=?, Phone=?, NameOfDad=?, NameOfMom=?, NumSiblings=?, NumChildren=?, Status=?, Job=?, Salary=?
		ORDER BY ID
		LIMIT 1
	`, updatedUser.FirstName, updatedUser.LastName, updatedUser.Age, updatedUser.Birthday, updatedUser.Email, updatedUser.Phone, updatedUser.NameOfDad, updatedUser.NameOfMom, updatedUser.NumSiblings, updatedUser.NumChildren, updatedUser.Status, updatedUser.Job, updatedUser.Salary)

	if err != nil {
		fmt.Println("Error updating first user record (Complexity 2):", err)
	}
}

// Additional processing for write operation in Complexity 3
func updateUserComplexity3(db *sql.DB) {
	// Generate random values for the update
	updatedUser := generateRandomUser()

	// Perform the SQL UPDATE operation on the first record
	_, err := db.Exec(`
		UPDATE user
		SET FirstName=?, LastName=?, Age=?, Birthday=?, Email=?, Phone=?, NameOfDad=?, NameOfMom=?, NumSiblings=?, NumChildren=?, Status=?, Job=?, Salary=?
		ORDER BY ID
		LIMIT 1
	`, updatedUser.FirstName, updatedUser.LastName, updatedUser.Age, updatedUser.Birthday, updatedUser.Email, updatedUser.Phone, updatedUser.NameOfDad, updatedUser.NameOfMom, updatedUser.NumSiblings, updatedUser.NumChildren, updatedUser.Status, updatedUser.Job, updatedUser.Salary)

	if err != nil {
		fmt.Println("Error updating first user record (Complexity 3):", err)
	}

	// Fetch and log the updated user's information
	readUserComplexity1(db)
}
