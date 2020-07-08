configure_file(site.yml site.yml COPYONLY)
if (DEV_ENV)
    configure_file(site_deploy.yml site_deploy.yml COPYONLY)
endif ()