<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:fn="http://www.w3.org/2005/xpath-functions"
                xmlns:a="http://www.openarchives.org/OAI/2.0/"
                xmlns:b="http://www.loc.gov/MARC21/slim"
                version="2.0" exclude-result-prefixes="#all">

  <xsl:output indent="no" method="xml" omit-xml-declaration="yes"/>
  <xsl:strip-space elements="*"/>

  <xsl:template match="/">
    <!-- select MARC21 records-->
    <xsl:for-each select="a:OAI-PMH/a:ListRecords/a:record/a:metadata//b:record">
      <!-- filter for rcords that were published before the year 2000
      xsl:if test="not(fn:starts-with(fn:substring(b:controlfield[@tag='008'],8,9),'2'))"-->
        <xsl:copy-of select="." copy-namespaces="yes"/><xsl:text>&#10;</xsl:text>
      <!--/xsl:if-->
    </xsl:for-each>
  </xsl:template>

</xsl:stylesheet>
