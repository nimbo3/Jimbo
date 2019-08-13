RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color
#printf "I ${YELLOW}love${NC} Stack Overflow\n"
echo crateing package file. please wait...
mvn package >>package.log 2>&1
if [ $? -eq 0 ]; then
  printf "${GREEN}Maven package created.${NC}\n"
else
  printf "${RED}Maven package creation failed.${NC}\n"
  read -p "Do you want to skip running tests(y/n)? " yn
  case $yn in
  [Nn]*)
    echo view package craetion log in: package.log
    exit
    ;;
  [Yy]*)
    mvn package -DskipTests >>package.log 2>&1
    if [ $? -eq 0 ]; then
      printf "${GREEN}Maven package created.${NC}\n"
    else
      printf "${RED}Maven package creation failed. view log file: package.log${NC}\n"
      exit
    fi
    ;;
  esac
fi
read -p 'Enter server`s address: ' address
echo remove existing jar on $address
ssh -p 3031 jimbo@$address 'rm ~/search-api-0.0.1-SNAPSHOT.jar' >package.log 2>&1
if [ $? -eq 0 ]; then
  echo Existing package removed from $address
else
  printf "${RED}Can not remove package.${NC} maybe not exists.\n"
fi
echo Start sending new package file to $address
scp -P 3031 target/search-api-0.0.1-SNAPSHOT.jar jimbo@$address:~
if [ $? -eq 0 ]; then
  printf "${GREEN}Package sent to server successfully.${NC}\n"
else
  printf "${RED}Can't send package to server.${NC}\n"
fi
