###########################################################################################
#  This is a very basic script which you can correct the 'perspective distortion' on XenoCam
#  images. The principle is to calibrate the image captured by the camera to an object and
#  a surface of known size. In this case we use the alignment plate.
#  Every pixel on the output image is 1 cm. 
#  This is not a perfect way for doing the calculations. But to an extent this will give 
#  good estimations of the geometry
#  Note that the objects which are not on the road surface will appear distorted because 
#  the matrix which is in the below code is adapted for the road surface.
#  The error will be more if you measure objects which are farther. This is because the 
#  XenoCam needs to be focussed for the road surface close to the car.
#
###########################################################################################

import cv2
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.widgets import Button
import os
import tqdm



# This matrix is fully adapted for your setup. If you change the orientation of the camera this value should change.
matrix = np.asarray([[-4.72213304e-01,6.07375445e+00, -2.36383184e+01],
 [ 9.15608405e-01,  2.65031461e+00, -7.51851357e+02],
 [-2.53338772e-04,  4.22432334e-03,  1.00000000e+00]])



# root = r'H:\INFRAPLUS2\Testsession2\Camera' #input folder
# out = r'H:\INFRAPLUS2\Testsession2\TxR' #output folder
root = r'D:\xenomatix\output\survey_data_20240726\PAVE\20240726_2\PAVE-0'
out = r'D:\xenomatix\output\survey_data_20240726\out'

if not os.path.exists(out):
    os.mkdir(out)
images = os.listdir(root)
for image_test in tqdm.tqdm(images):
    path = os.path.join(root,image_test)
    if path.endswith('.jpg'):
        img_2 = cv2.imread(path)
        img_2 = cv2.resize(img_2, (0,0), fx=0.5, fy=0.5)
        corrected_img = cv2.cvtColor(cv2.warpPerspective(img_2, matrix, (1200, 1200)),cv2.COLOR_BGR2RGB)
        #plt.imshow(cv2.cvtColor(corrected_img, cv2.COLOR_BGR2RGB))
        #plt.title('Corrected Image')
        #plt.axis('off')
        #plt.show()
        cv2.imwrite(os.path.join(out,image_test),cv2.cvtColor(corrected_img, cv2.COLOR_BGR2RGB))
       

