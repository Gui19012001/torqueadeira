[app]

# Nome do aplicativo
title = Torque PF6000
package.name = torquepf6000
package.domain = br.com.ibero

# Arquivo principal
source.dir = .
source.include_exts = py,kv,png,jpg,jpeg,json,txt,zpl

# Versao
version = 0.1.0

# Dependencias Python
requirements = python3,kivy,pyjnius,android

# Orientacao para tablet
orientation = landscape
fullscreen = 0

# Permissoes
android.permissions = INTERNET

# Android
android.api = 34
android.minapi = 24
android.ndk = 25b
android.archs = arm64-v8a, armeabi-v7a
android.accept_sdk_license = True

# USB Host: recurso usado para Zebra USB/OTG
android.add_manifest_xml = android_extra_manifest.xml

# Tema / janela
android.presplash_color = #0B1020

# Build
log_level = 2
warn_on_root = 1

[buildozer]
log_level = 2
warn_on_root = 1
