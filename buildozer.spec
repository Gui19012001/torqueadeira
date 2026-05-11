[app]
title = Torque PF6000
package.name = torquepf6000
package.domain = br.com.suaempresa

source.dir = .
source.include_exts = py,kv,png,jpg,jpeg,json,txt,env,zpl,xml
source.exclude_dirs = .git,.github,__pycache__,bin,.buildozer,venv

version = 0.1.3

requirements = python3,kivy,pyjnius

orientation = landscape
fullscreen = 0

android.permissions = INTERNET
android.api = 34
android.minapi = 24
android.archs = arm64-v8a, armeabi-v7a
android.accept_sdk_license = True

# Deixei sem android.add_manifest_xml para ficar igual ao padrão dos seus APKs que já funcionaram.
# A impressão USB continua sendo testada via UsbManager/pyjnius dentro do app.
# Se depois precisar forçar USB Host no manifesto, descomente a linha abaixo:
# android.add_manifest_xml = android_extra_manifest.xml

[buildozer]
log_level = 2
warn_on_root = 0
