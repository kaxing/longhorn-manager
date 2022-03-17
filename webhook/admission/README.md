# Admission Webhook

The admission webhook validates or changes the values in a request.
An resource request from longhorn-manager might be handled by an
admission webhook on another node. Therefore, any manipulation of
the resources of a node, such as creating and deleting folders,
should be avoided. The manipulations should be handled by the 
longhorn-manager daemon or controller.
