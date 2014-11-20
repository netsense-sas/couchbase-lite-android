source build_automation.rb

if [ -z "MAVEN_UPLOAD_VERSION" ];
then
	echo "Need do set ${MAVEN_UPLOAD_VERSION}";
        exit 11
fi

if [ -z "MAVEN_UPLOAD_USERNAME" ];
then
	echo "Need do set ${MAVEN_UPLOAD_USERNAME}";
        exit 22
fi

if [ -z "MAVEN_UPLOAD_PASSWORD" ];
then
	echo "Need do set ${MAVEN_UPLOAD_PASSWORD}";
        exit 33
fi

if [ -z "MAVEN_UPLOAD_REPO_URL" ];
then
	echo "Need do set ${MAVEN_UPLOAD_REPO_URL}";
        exit 44
fi

ruby -r "./build_automation.rb" -e "uploadArchives"

