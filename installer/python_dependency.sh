
if [ -f /var/lib/jenkins ];
then 
	echo "Updating Bastion instance ... "
	pip3 install -r requirements_no_AI.txt
else
	if [ -f ~/Documents/droidDev ];
	then
		echo "Updating PC instance ... "
		pip3 install -r requirements_AI.txt
		pip3 uninstall numpy -y
		pip3 install numpy
	else
		echo "Updating without AI ..."
		pip3 install -r requirements_no_AI.txt
	fi
fi