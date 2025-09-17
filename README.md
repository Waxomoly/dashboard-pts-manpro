## Table of Contents
- [Installation](#installation)
- [Maintainance](#maintainance)

## Installation

Follow these steps to get your development environment set up.

1.  Clone the repository to your local machine:
    ```bash
    git clone [https://github.com/your-username/your-repo-name.git](https://github.com/your-username/your-repo-name.git)
    ```
2.  Navigate to the project directory:
    ```bash
    cd your-repo-name
    ```
3.  Run virtual environment (ini supaya perubahan dependency kalian g ngaruh ke python global di laptop kalian, 
    apapun command terminal setelah ini, ketik di terminal yg kegenerate dengan command ini):
    ```bash
    .\.venv\Scripts\activate
    ```
4. Install the required dependencies using pip:
    ```bash
    pip install -r requirements.txt
    ```



## Maintainance

1.  Everytime you add dependency, run this command to update requirements.txt
    ```bash
        pip freeze > requirements.txt
    ```